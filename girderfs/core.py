# -*- coding: utf-8 -*-
from collections import defaultdict

import os
from stat import S_IFDIR
from time import time

from fuse import Operations, LoggingMixIn

FILE_MARKER = '<files>'


def _mapListToKeys(dataDict, mapList):
    if not mapList:
        return dataDict
    return _mapListToKeys(dataDict[mapList[0]], mapList[1:])


def _attach(branch, trunk):
    '''
    Insert a branch of directories on its trunk.
    http://stackoverflow.com/questions/8484943
    '''
    parts = branch.split('/', 1)
    if len(parts) == 1:  # branch is a file
        trunk[FILE_MARKER].append(parts[0])
    else:
        node, others = parts
        if node not in trunk:
            trunk[node] = defaultdict(dict, ((FILE_MARKER, []),))
        _attach(others, trunk[node])


class GirderFS(LoggingMixIn, Operations):
    'Example filesystem to demonstrate fuse_get_context()'

    def __init__(self, folderId, gc):
        super(GirderFS, self).__init__()
        self.folderId = folderId
        self.gc = gc

    def _refreshData(self):
        self.data = self.gc.get('folder/%s/contents' % self.folderId)

    def _getDirs(self):
        self._refreshData()
        dirs = defaultdict(dict, ((FILE_MARKER, []),))
        for item in list(self.data.keys()):
            _attach(item, dirs)
        return dirs

    def getattr(self, path, fh=None):
        if path == '/':
            return dict(st_mode=(S_IFDIR | 0o755), st_nlink=2)

        self._refreshData()
        if path[1:] in list(self.data.keys()):
            st = os.lstat(self.data[path[1:]])
            return dict((key, getattr(st, key))
                        for key in ('st_atime', 'st_ctime', 'st_gid',
                                    'st_mode', 'st_mtime', 'st_nlink',
                                    'st_size', 'st_uid'))
        else:
            st = dict(st_mode=(S_IFDIR | 0o755), st_nlink=2)
            st['st_ctime'] = st['st_mtime'] = st['st_atime'] = time()
        return st

    def read(self, path, size, offset, fh):
        fh = os.open(self.data[path[1:]], os.O_RDONLY)
        os.lseek(fh, offset, 0)
        return os.read(fh, size)

    def release(self, path, fh):
        return os.close(fh)

    def _get_objects(self, path):
        dirs = self._getDirs()
        if path == '/':
            ldirs = list(dirs.keys())
            files = dirs[FILE_MARKER]
        else:
            mapPath = path[1:].split('/')
            ldirs = list(_mapListToKeys(dirs, mapPath).keys())
            mapPath.append(FILE_MARKER)
            files = _mapListToKeys(dirs, mapPath)
        ldirs.remove(FILE_MARKER)
        return (ldirs, files)

    def readdir(self, path, fh):
        dirents = ['.', '..']
        self._refreshData()
        ldirs, files = self._get_objects(path)
        return dirents + ldirs + files

    # Disable unused operations:
    access = None
    flush = None
    getxattr = None
    listxattr = None
    opendir = None
    open = None
    releasedir = None
    statfs = None


if __name__ == '__main__':
    import girder_client
    import logging
    from sys import argv, exit
    from fuse import FUSE
    girder_api_url = os.environ.get('GIRDER_API_URL',
                                    'http://localhost:8080/api/v1')
    try:
        girder_api_key = os.environ['GIRDER_API_KEY']
    except KeyError:
        print('You need to set env var with girder api key')
        raise

    if len(argv) != 3:
        print('usage: %s <folderId> <mountpoint>' % argv[0])
        print('exampe: %s 57716a4b37025b0001078154 /tmp/myfs' % argv[0])
        exit(1)

    gc = girder_client.GirderClient(apiUrl=girder_api_url)
    gc.authenticate(apiKey=girder_api_key)
    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(GirderFS(argv[1], gc), argv[2], foreground=True, ro=True)