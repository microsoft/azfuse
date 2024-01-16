from .common import exclusive_open_to_read
from .common import robust_open_to_write
from .common import get_file_size
from .cloud_storage import create_cloud_fuse
from .common import get_mpi_local_rank
from .common import get_azfuse_env
import glob
import os.path as op
import os
import contextlib


class File(object):
    initialized = False
    use_fuser = False
    fuser = None

    @classmethod
    def ensure_initialized(cls):
        if not cls.initialized:
            cls.initialized = True
            # TSV_USE_FUSE is only for back-compatibility
            cls.use_fuser = int(get_azfuse_env('TSV_USE_FUSE', '0')) + int(get_azfuse_env('USE_FUSE', '0'))
            if cls.use_fuser:
                cls.fuser = create_cloud_fuse()
                gc  = int(get_azfuse_env('USE_FUSE_ENABLE_GARBAGE_COLLECTION', '0'))
                if gc and get_mpi_local_rank() == 0:
                    cls.fuser.ensure_invoke_garbage_collect()

    @classmethod
    def isfile(cls, path):
        cls.ensure_initialized()
        if cls.use_fuser:
            return cls.fuser.isfile(path)
        else:
            return op.isfile(path)

    @classmethod
    def open(cls, fname, mode='r'):
        cls.ensure_initialized()
        if mode in ['r', 'rb']:
            if cls.use_fuser:
                return cls.fuser.open(fname, mode)
            else:
                return exclusive_open_to_read(fname, mode)
        elif mode in ['w', 'wb']:
            if cls.use_fuser:
                return cls.fuser.open(fname, mode)
            else:
                return robust_open_to_write(fname, mode)

    @classmethod
    def clear_cache(cls, folder):
        cls.ensure_initialized()
        if not cls.use_fuser:
            return
        return cls.fuser.clear_cache(folder)

    @classmethod
    def rm(cls, fname):
        cls.ensure_initialized()
        if cls.use_fuser:
            cls.fuser.rm(fname)
        else:
            os.remove(fname)

    @classmethod
    def async_upload(cls, enabled=False, shm_as_tmp=False):
        cls.ensure_initialized()
        if not cls.use_fuser:
            return contextlib.nullcontext()
        return cls.fuser.async_upload(enabled, shm_as_tmp)

    @classmethod
    @property
    def async_upload_queue(cls):
        cls.ensure_initialized()
        if not cls.use_fuser:
            return
        return cls.fuser.async_upload_queue

    @classmethod
    def get_file_size(cls, fname):
        cls.ensure_initialized()
        if cls.use_fuser:
            return cls.fuser.get_file_size(fname)
        else:
            return get_file_size(fname)

    @classmethod
    def list(cls, folder, recursive=False, return_info=False):
        cls.ensure_initialized()
        if cls.use_fuser:
            return cls.fuser.list(folder, recursive=recursive,
                                  return_info=return_info)
        else:
            assert not return_info
            return glob.glob(op.join(folder, '*'), recursive=recursive)

    @classmethod
    def walk(cls, folder, return_info=False):
        cls.ensure_initialized()
        if cls.use_fuser:
            return cls.fuser.walk(folder, recursive=recursive,
                                  return_info=return_info)
        else:
            assert not return_info
            return glob.glob(op.join(folder, '*'), recursive=recursive)

    @classmethod
    def prepare(cls, file_or_fnames):
        if isinstance(file_or_fnames, str):
            file_or_fnames = [file_or_fnames]
        fnames = file_or_fnames
        cls.ensure_initialized()
        if cls.use_fuser:
            cls.fuser.ensure_cache(fnames)

    @classmethod
    def set_access_tier(cls, fname, tier):
        cls.ensure_initialized()
        if cls.use_fuser:
            cls.fuser.set_access_tier(fname, tier)

    @classmethod
    def upload(cls, cache, remote):
        cls.ensure_initialized()
        if cls.use_fuser:
            cls.fuser.upload(cache, remote)

    @classmethod
    def get_cache_file(cls, file_name):
        cls.ensure_initialized()
        if cls.use_fuser:
            info = cls.fuser.get_remote_cache(file_name)
            if info:
                return op.join(info['cache'], info['sub_name'])
            else:
                return file_name
        else:
            return file_name
