# -*- coding: utf-8 -*-
import argparse
import os
from ctypes import cdll
from fuse import FUSE
from girder_client import GirderClient

from girderfs.core import \
    RESTGirderFS, LocalGirderFS


_libc = cdll.LoadLibrary('libc.so.6')
_setns = _libc.setns
CLONE_NEWNS = 0x00020000


def setns(fd, nstype):
    if hasattr(fd, 'fileno'):
        fd = fd.fileno()
    _setns(fd, nstype)


def main(args=None):
    parser = argparse.ArgumentParser(
        description='Mount Girder filesystem assetstore.')
    parser.add_argument('--api-url', required=True, default=None,
                        help='full URL to the RESTful API of Girder server')
    parser.add_argument('--username', required=False, default=None)
    parser.add_argument('--password', required=False, default=None)
    parser.add_argument('--api-key', required=False, default=None)
    parser.add_argument('--token', required=False, default=None)
    parser.add_argument('-c', default='remote', choices=['remote', 'direct'],
                        help='command to run')
    parser.add_argument('--foreground', dest='foreground',
                        action='store_true')
    parser.add_argument('--hostns', dest='hostns', action='store_true')
    parser.add_argument('local_folder', help='path to local target folder')
    parser.add_argument('remote_folder', help='Girder\'s folder id')

    args = parser.parse_args()

    gc = GirderClient(apiUrl=args.api_url)
    if args.token:
        gc.token = args.token
    elif args.api_key:
        gc.authenticate(apiKey=args.api_key)
    elif args.username and args.password:
        gc.authenticate(username=args.username, password=args.password)
    else:
        raise RuntimeError("You need to specify apiKey or user/pass")

    if args.hostns:
        targetns = os.path.join(os.environ.get('HOSTDIR', '/'),
                                'proc/1/ns/mnt')
        with open(targetns) as fd:
            setns(fd, CLONE_NEWNS)

    if args.c == 'remote':
        FUSE(RESTGirderFS(args.remote_folder, gc), args.local_folder,
             foreground=args.foreground, ro=True, allow_other=True)
    elif args.c == 'direct':
        FUSE(LocalGirderFS(args.remote_folder, gc), args.local_folder,
             foreground=args.foreground, ro=True, allow_other=True)
    else:
        print('No implementation for command %s' % args.c)


if __name__ == "__main__":
    main()
