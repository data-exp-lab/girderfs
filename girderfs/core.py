# -*- coding: utf-8 -*-
import os
import six
import time
import pathlib
import encodings.idna  # NOQA: http://stackoverflow.com/questions/9144724/
import requests
from stat import S_IFDIR, S_IFREG
from errno import ENOENT
from dateutil.parser import parse as tparse
from fuse import Operations, LoggingMixIn, FuseOSError
import girder_client


def _lstrip_path(path):
    pathObj = pathlib.Path(path)
    return pathlib.Path(*pathObj.parts[1:])


def _convert_time(strtime):
    return tparse(strtime).timestamp()


class GirderFS(LoggingMixIn, Operations):

    def __init__(self, folderId, gc):
        super(GirderFS, self).__init__()
        self.folderId = folderId
        self.gc = gc
        self.cache = {}

    def _get_object_by_path(self, objId, path):
        raw_listing = self._get_listing(objId)
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

    def _get_listing(self, objId):
        try:
            return self.cache[objId]
        except KeyError:
            try:
                self.cache[objId] = self.gc.get('folder/%s/listing' % objId)
            except girder_client.HttpError:
                self.cache[objId] = self.gc.get('item/%s/listing' % objId)
        finally:
            return self.cache[objId]

    def getattr(self, path, fh=None):
        if path == '/':
            return dict(st_mode=(S_IFDIR | 0o755), st_nlink=2)
        obj, objType = self._get_object_by_path(
            self.folderId, _lstrip_path(path))

        if objType == 'folder':
            st = dict(st_mode=(S_IFDIR | 0o755), st_nlink=2)
        else:
            st = dict(st_mode=(S_IFREG | 0o644), st_nlink=1)
        ctime = _convert_time(obj["created"])
        try:
            mtime = _convert_time(obj["updated"])
        except KeyError:
            mtime = ctime
        st.update(dict(st_ctime=ctime, st_mtime=mtime,
                       st_size=obj["size"], st_atime=time.time()))
        return st

    def read(self, path, size, offset, fh):
        raise NotImplemented

    def readdir(self, path, fh):
        dirents = ['.', '..']
        if path == '/':
            raw_listing = self._get_listing(self.folderId)
        else:
            obj, objType = self._get_object_by_path(
                self.folderId, _lstrip_path(path))
            raw_listing = self._get_listing(obj["_id"])

        for objType in raw_listing.keys():
            dirents += [_["name"] for _ in raw_listing[objType]]
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

    :param folderId: Folder id
    :type folderId: str
    :param gc: Authenticated instance of GirderClient
    :type gc: girder_client.GriderClient
    """

    def read(self, path, size, offset, fh):
        obj, objType = self._get_object_by_path(
            self.folderId, _lstrip_path(path))

        content = six.BytesIO()
        req = requests.get(
            '%sfile/%s/download' % (self.gc.urlBase, obj["_id"]),
            headers={'Girder-Token': self.gc.token},
            params={'offset': offset, 'endByte': offset + size})
        for chunk in req.iter_content(chunk_size=65536):
            content.write(chunk)
        return content.getvalue()


class LocalGirderFS(GirderFS):
    """
    Filesystem for mounting local Girder's FilesystemAssetstore

    :param folderId: Folder id
    :type folderId: str
    :param gc: Authenticated instance of GirderClient
    :type gc: girder_client.GriderClient
    """

    def read(self, path, size, offset, fh):
        obj, objType = self._get_object_by_path(
            self.folderId, _lstrip_path(path))
        fh = os.open(obj['path'], os.O_RDONLY)
        os.lseek(fh, offset, 0)
        return os.read(fh, size)

    def release(self, path, fh):
        return os.close(fh)
