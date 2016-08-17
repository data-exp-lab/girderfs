# -*- coding: utf-8 -*-
"""
Core classes for FUSE based filesystem handling Girder's resources
"""

import os
import time
import pathlib
from stat import S_IFDIR, S_IFREG
from errno import ENOENT
# http://stackoverflow.com/questions/9144724/
import encodings.idna  # NOQA pylint: disable=unused-import
import requests
import six
from dateutil.parser import parse as tparse
from fuse import Operations, LoggingMixIn, FuseOSError
import girder_client


def _lstrip_path(path):
    path_obj = pathlib.Path(path)
    return pathlib.Path(*path_obj.parts[1:])


def _convert_time(strtime):
    return tparse(strtime).timestamp()


class GirderFS(LoggingMixIn, Operations):
    """
    Base class for handling Girder's folders

    :param folder_id: Folder id
    :type folder_id: str
    :param gc: Authenticated instance of GirderClient
    :type gc: girder_client.GriderClient
    """

    def __init__(self, folder_id, girder_cli):
        super(GirderFS, self).__init__()
        self.folder_id = folder_id
        self.girder_cli = girder_cli
        self.cache = {}

    def _get_object_by_path(self, obj_id, path):
        raw_listing = self._get_listing(obj_id)
        folder = next((item for item in raw_listing['folders']
                       if item["name"] == path.parts[0]), None)

        if folder is not None:
            if len(path.parts) == 1:
                return folder, "folder"
            else:
                return self._get_object_by_path(folder["_id"],
                                                pathlib.Path(*path.parts[1:]))

        _file = next((item for item in raw_listing['files']
                      if item["name"] == path.parts[0]), None)
        if _file is not None:
            return _file, "file"

        return FuseOSError(ENOENT)

    def _get_listing(self, obj_id):
        try:
            return self.cache[obj_id]
        except KeyError:
            try:
                self.cache[obj_id] = self.girder_cli.get('folder/%s/listing' % obj_id)
            except girder_client.HttpError:
                self.cache[obj_id] = self.girder_cli.get('item/%s/listing' % obj_id)
        finally:
            return self.cache[obj_id]

    def getattr(self, path, fh=None):
        if path == '/':
            return dict(st_mode=(S_IFDIR | 0o755), st_nlink=2)
        obj, obj_type = self._get_object_by_path(
            self.folder_id, _lstrip_path(path))

        if obj_type == 'folder':
            stat = dict(st_mode=(S_IFDIR | 0o755), st_nlink=2)
        else:
            stat = dict(st_mode=(S_IFREG | 0o644), st_nlink=1)
        ctime = _convert_time(obj["created"])
        try:
            mtime = _convert_time(obj["updated"])
        except KeyError:
            mtime = ctime
        stat.update(dict(st_ctime=ctime, st_mtime=mtime,
                         st_size=obj["size"], st_atime=time.time()))
        return stat

    def read(self, path, size, offset, fh):
        raise NotImplementedError

    def readdir(self, path, fh):
        dirents = ['.', '..']
        if path == '/':
            raw_listing = self._get_listing(self.folder_id)
        else:
            obj, obj_type = self._get_object_by_path(
                self.folder_id, _lstrip_path(path))
            raw_listing = self._get_listing(obj["_id"])

        for obj_type in raw_listing.keys():
            dirents += [_["name"] for _ in raw_listing[obj_type]]
        return dirents

    # Disable unused operations:
    access = None
    flush = None
    getxattr = None
    listxattr = None
    opendir = None
    open = None
    release = None
    releasedir = None
    statfs = None


class RESTGirderFS(GirderFS):
    """
    Filesystem for locally mounting a remote Girder folder

    :param folder_id: Folder id
    :type folder_id: str
    :param gc: Authenticated instance of GirderClient
    :type gc: girder_client.GriderClient
    """

    def read(self, path, size, offset, fh):
        obj, _ = self._get_object_by_path(
            self.folder_id, _lstrip_path(path))

        content = six.BytesIO()
        req = requests.get(
            '%sfile/%s/download' % (self.girder_cli.urlBase, obj["_id"]),
            headers={'Girder-Token': self.girder_cli.token},
            params={'offset': offset, 'endByte': offset + size})
        for chunk in req.iter_content(chunk_size=65536):
            content.write(chunk)
        return content.getvalue()


class LocalGirderFS(GirderFS):
    """
    Filesystem for mounting local Girder's FilesystemAssetstore

    :param folder_id: Folder id
    :type folder_id: str
    :param gc: Authenticated instance of GirderClient
    :type gc: girder_client.GriderClient
    """

    def read(self, path, size, offset, fh):
        obj, _ = self._get_object_by_path(
            self.folder_id, _lstrip_path(path))
        fh = os.open(obj['path'], os.O_RDONLY)
        os.lseek(fh, offset, 0)
        return os.read(fh, size)

    def release(self, path, fh):  # pylint: disable=unused-argument
        return os.close(fh)
