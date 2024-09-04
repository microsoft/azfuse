from .common import list_to_dict
import contextlib
from .common import query_all_opened_file_in_system
from .common import ensure_remove_file
from .common import has_handle
from .common import get_file_size
from .common import hash_sha1
from .common import acquireLock, hash_sha1, releaseLock
from .common import write_to_file, wait_if_zero_file_size
from .common import limited_retry_agent
from .common import get_azfuse_env
import shutil
import multiprocessing as mp
import os.path as op
from .common import ensure_remove_dir
import glob
from pprint import pformat
from .common import load_from_yaml_file
from .common import cmd_run
import subprocess as sp
from .common import parse_iteration
from .common import logger
import logging
import time
from tqdm import tqdm
import datetime
import os
from .common import ensure_directory
from deprecated import deprecated
import io

logging.getLogger("azure.storage.common.storageclient").setLevel(logging.WARNING)
logging.getLogger('azure.storage').setLevel(logging.WARNING)
logging.getLogger('azure.core.pipeline.policies.http_logging_policy').setLevel(logging.WARNING)

@contextlib.contextmanager
def robust_open_to_write(fname, mode):
    tmp = fname + '.tmp'
    ensure_directory(op.dirname(tmp))
    with io.open(tmp, mode) as fp:
        yield fp
    os.rename(tmp, fname)

def create_cloud_storage(x=None, config_file=None, config=None):
    if config is not None:
        return CloudStorage(config)
    if config_file is None:
        folder = get_azfuse_env(
            'STORAGE_ACCOUNT_CONFIG_FOLDER',
            './aux_data/storage_account',
        )
        config_file = op.join(folder, '{}.yaml'.format(x))
    config = load_from_yaml_file(config_file)
    c = CloudStorage(config)
    return c

@deprecated()
def azcopy_upload(src, dest_url, dest_key):
    cmd = [get_azcopy(), '--source',
            src,
            '--destination',
            dest_url,
            '--exclude-older',
            '--dest-key',
            "{}".format(dest_key),
            '--quiet',
            '--parallel-level',
            '32']
    resume_file = '/tmp/azure.' + hash_sha1([src, dest_url]) + '.jnl'
    cmd.append('--resume')
    cmd.append(resume_file)
    if op.isdir(src):
        cmd.append('--recursive')
    cmd_run(cmd, shell=True)

def blob_upload_qdoutput(src_path, dest_path, client):
    to_copy = get_to_copy_file_for_qdoutput(src_path, dest_path)
    for f, d in to_copy:
        client.az_upload2(f, d)

def get_to_copy_file_for_qdoutput(src_path, dest_path):
    # upload all the files under src_path
    to_copy = []
    all_src_file = glob.glob(op.join(src_path, '*'))
    exclude_suffix = ['txt', 'zip']
    for f in all_src_file:
        if op.isfile(f) and not any(f.endswith(s) for s in exclude_suffix):
            to_copy.append((f, op.join(dest_path, op.basename(f))))

    # for the model and the tested files, only upload the best
    all_src_file = glob.glob(op.join(src_path, 'snapshot',
        'model_iter_*.caffemodel'))
    all_src_file.extend(glob.glob(op.join(src_path, 'snapshot',
        'model_iter_*.pt')))
    all_iter = [parse_iteration(f) for f in all_src_file]
    max_iters = max(all_iter)
    need_copy_files = [f for f, i in zip(all_src_file, all_iter) if i == max_iters]
    dest_snapshot = op.join(dest_path, 'snapshot')
    for f in need_copy_files:
        to_copy.append((f, op.join(dest_snapshot, op.basename(f))))
        if f.endswith('.caffemodel'):
            f = f.replace('.caffemodel', '.solverstate')
            to_copy.append((f, op.join(dest_snapshot, op.basename(f))))
    return to_copy

def blob_upload(src, dst, c=None):
    if c is None:
        c = create_cloud_storage('vig')
    c.az_upload2(src, dst)

def get_root_all_full_expid(full_expid_prefix, all_blob_name):
    # full_expid_prefix can be the folder of full_expid; or with some prefix so
    # that we can filter
    if all(b.startswith(full_expid_prefix + '/') for b in all_blob_name):
        root = full_expid_prefix
    else:
        root = op.dirname(full_expid_prefix)
    all_full_expid = set(b[len(root) + 1:].split('/')[0] for b in
            all_blob_name if b.startswith(root))
    return root, all_full_expid

def get_azcopy():
    # this is v10
    azcopy = op.expanduser('~/code/azcopy/azcopy')
    if not op.isfile(azcopy):
        azcopy = 'azcopy'
    return azcopy

def get_leaf_names(all_fname):
    # build the tree first
    from ete3 import Tree
    root = Tree()
    for fname in all_fname:
        components = fname.split('/')
        curr = root
        for com in components:
            currs = [c for c in curr.children if c.name == com]
            if len(currs) == 0:
                curr = curr.add_child(name=com)
            else:
                assert len(currs) == 1
                curr = currs[0]
    result = []
    for node in root.iter_leaves():
        ans = [s.name for s in node.get_ancestors()[:-1]]
        ans.insert(0, node.name)
        result.append('/'.join([a for a in ans[::-1]]))
    return result

def blob_download_qdoutput(src_path, target_folder):
    c = create_cloud_storage('vig')
    c.blob_download_qdoutput(src_path, target_folder)

def create_cloud_fuse(config=None):
    if config is None:
        fname = get_azfuse_env(
            'CLOUD_FUSE_CONFIG_FILE',
            'aux_data/configs/azfuse.yaml',
        )
        logger.info(f'init azfuse from {fname}')
        config = load_from_yaml_file(fname)
    assert not isinstance(config, dict)
    return AzFuse(config)

def garbage_collection_for_cloud_fuse_loop(local_folders, total_size_limit):
    logger.info('size limit = {}'.format(total_size_limit))
    from .common import find_mount_point, list_to_dict
    mount_path = [(find_mount_point(l), l) for l in local_folders]
    mount2paths = list_to_dict(mount_path, 0)
    while True:
        try:
            for m, paths in mount2paths.items():
                curr_limit = total_size_limit
                if curr_limit < 1:
                    x = shutil.disk_usage(m)
                    curr_limit = x.total * total_size_limit
                garbage_collection_for_cloud_fuse(paths, curr_limit)
        except:
            from .common import print_trace
            print_trace()
        time.sleep(10)

def garbage_collection_for_cloud_fuse(local_folders, total_size_limit):
    infos = []
    for local_folder in local_folders:
        for root, _, files in os.walk(local_folder):
            for f in files:
                if f.startswith('.') or f.endswith('.tmp'):
                    continue
                f = op.join(root, f)
                if op.isfile(f):
                    info = {
                        'fname': f,
                        'size_in_bytes': get_file_size(f),
                        'last_access_time': op.getatime(f),
                    }
                    infos.append(info)
    we_used_total = sum(i['size_in_bytes'] for i in infos)
    if we_used_total < total_size_limit:
        return
    # we need to delete
    need_to_delete = we_used_total - total_size_limit

    opened = set(query_all_opened_file_in_system())
    logger.info('queried {} opened files in system'.format(len(opened)))
    for i in infos:
        i['has_handle'] = has_handle(i['fname'], opened)
    infos = sorted(infos, key=lambda i: i['last_access_time'])

    deleted = []
    skip_deleted = []
    for i in infos:
        if i['has_handle']:
            skip_deleted.append(i['fname'])
        else:
            ensure_remove_file(i['fname'])
            need_to_delete -= i['size_in_bytes']
            deleted.append(i['fname'])
            if need_to_delete < 0:
                break
    logger.info(
        'total size limit = {},'
        'need to delete = {},'
        'all occupied = {},'
        'deleted = {},'
        'skip deleted = {}'
        .format(
            total_size_limit,
            we_used_total - total_size_limit,
            sum([i['size_in_bytes'] for i in infos]),
            pformat(deleted),
            pformat(skip_deleted),
        )
    )

def async_upload_thread_entry(async_upload_queue):
    time_to_exit = False
    account2cloud = {}
    while not time_to_exit:
        all_info = []
        total_size = 0
        r = async_upload_queue.get()
        if r == None:
            time_to_exit = True
        else:
            all_info.append(r)
            if r.get('clear_cache_after_upload'):
                fsize = get_file_size(op.join(r['cache'], r['sub_name']))
                total_size += fsize or 0
            while not async_upload_queue.empty():
                r2 = async_upload_queue.get()
                if r2 is None:
                    time_to_exit = True
                    break
                else:
                    all_info.append(r2)
                    if r2.get('clear_cache_after_upload'):
                        fsize = get_file_size(op.join(r2['cache'], r2['sub_name']))
                        total_size += fsize or 0
                    if total_size > 1024 * 1024 * 1024 * 10:
                        logger.info('there might be too many files to write. let us upload first')
                        break
        if len(all_info) > 0:
            async_upload_files(all_info, account2cloud)

def async_upload_files(infos, account2cloud):
    from pprint import pformat
    logger.debug('start async upload {}'.format(pformat(infos)))
    lock_fd = acquire_azcopy_lock()
    logger.debug('lock acquired {}'.format(pformat(infos)))

    config_infos = [(hash_sha1(pformat(info['config_info'])), info) for info in infos]
    config2infos = list_to_dict(config_infos, 0)

    for infos in config2infos.values():
        sns = [i['path_relative_to_config'] for i in infos]
        config = infos[0]['config_info']
        rd = config['remote']
        cd = config['cache']

        ac = config['storage_account']
        sns = list(set(sns))
        if ac not in account2cloud:
            account2cloud[ac] = create_cloud_storage(ac)
        if op.basename(rd[:-1] if rd.endswith('/') else rd) == op.basename(cd[:-1] if cd.endswith('/') else cd):
            file_list = '/tmp/{}'.format(hash_sha1(pformat(sns)))
            write_to_file('\n'.join(sns), file_list)
            for s in sns:
                assert op.isfile(op.join(cd, s)), (cd, s)
            limited_retry_agent(-1, account2cloud[ac].upload,
                cd,
                rd,
                file_list=file_list,
            )
            ensure_remove_file(file_list)
        else:
            for sn in sns:
                account2cloud[ac].upload(
                    op.join(cd, sn),
                    op.join(rd, sn),
                )
    releaseLock(lock_fd)
    for i in infos:
        if i.get('clear_cache_after_upload'):
            ensure_remove_file(op.join(i['cache'], i['sub_name']))
    logger.debug('finished async upload {}'.format(pformat(infos)))

def get_azcopy_lock_file():
    # az download/upload should share one lock
    return op.join('/tmp', 'lock_azcopy_fuse')

def acquire_azcopy_lock():
    lock_fd = acquireLock(get_azcopy_lock_file())
    return lock_fd

def download_worker_entry(queue):
    fuser = create_cloud_fuse()
    while True:
        fs = queue.pop(10)
        fuser.ensure_cache(fs)

def launch_async_upload_thread(queue):
    p = mp.Process(target=async_upload_thread_entry, args=(queue,))
    p.start()
    return p

class AzFuse(object):
    def __init__(self, config):
        # config is a list of dictionary (local, remote, cache, storage_account)
        for c in config:
            c['local'] = op.abspath(c['local'])
            c['cache'] = op.expanduser(c['cache'])
        self.config = config

        accounts = set([c['storage_account'] for c in self.config])
        self.account2cloud = {a: create_cloud_storage(a) for a in accounts}

        self.invoked_collector = False
        self.garbage_collector = None
        self.async_upload_enabled = False
        self.async_upload_queue = None
        self.async_upload_thread = None
        self.remove_cache_after_upload = False
        self.shm_as_upload_tmp = False

        self._download_queue = None
        self._download_process = None

        #self._ignore_cache = False

    def create_download_process(self, clear_queue=True):
        if self._download_process:
            return
        if clear_queue:
            self.download_queue.clear()
        p = mp.Process(target=download_worker_entry, args=(self.download_queue,),
                       daemon=True,
                       )
        p.start()
        self._download_process = p

    @property
    def download_queue(self):
        if self._download_queue is None:
            folder = '/tmp/azfuse_download'
            from .common import FileQueue
            ensure_directory(folder)
            self._download_queue = FileQueue(folder)
        return self._download_queue

    def request_to_download(self, fs):
        self.download_queue.put(fs)


    def get_url(self, fname):
        info = self.get_remote_cache(fname)
        remote_file = op.join(info['remote'], info['sub_name'])
        return self.account2cloud[info['storage_account']].get_url(remote_file)

    def get_meta(self, fname):
        info = self.get_remote_cache(fname)
        remote_file = op.join(info['remote'], info['sub_name'])
        return self.account2cloud[info['storage_account']].get_metadata(remote_file)

    def set_meta(self, meta, fname):
        info = self.get_remote_cache(fname)
        remote_file = op.join(info['remote'], info['sub_name'])
        return self.account2cloud[info['storage_account']].set_metadata(meta, remote_file)

    def isfile(self, fname):
        info = self.get_remote_cache(fname)
        if len(info) == 0:
            return op.isfile(fname)
        remote_file = op.join(info['remote'], info['sub_name'])
        return self.account2cloud[info['storage_account']].file_exists(remote_file)

    def clear_cache_related(self, cache_file):
        ensure_remove_file(cache_file)

    def is_cache_file_ready(self, cache_file):
        return op.isfile(cache_file)

    def is_cached(self, fname):
        info = self.get_remote_cache(fname)
        if len(info) == 0:
            return True
        return self.is_cache_file_ready(op.join(info['cache'], info['sub_name']))

    def rm(self, fname):
        info = self.get_remote_cache(fname)
        if len(info) == 0:
            return os.remove(fname)
        remote_file = op.join(info['remote'], info['sub_name'])
        cache_file = op.join(info['cache'], info['sub_name'])
        if self.is_cache_file_ready(cache_file):
            os.remove(cache_file)
        return self.account2cloud[info['storage_account']].rm(remote_file)

    def send_to_async_upload(self, fname_or_fnames, clear_cache_after_upload=False):
        if isinstance(fname_or_fnames, str):
            fnames = [fname_or_fnames]
        else:
            fnames = fname_or_fnames
        infos = [self.get_remote_cache(fname) for fname in fnames]
        assert self.async_upload_queue is not None
        for info in infos:
            if clear_cache_after_upload:
                info['clear_cache_after_upload'] = True
            self.async_upload_queue.put(info)

    @contextlib.contextmanager
    def async_upload(self, enabled, shm_as_tmp=False):
        old_enabled = self.async_upload_enabled
        old_shm = self.shm_as_upload_tmp
        old_async_upload_queue = self.async_upload_queue
        old_async_upload_thread = self.async_upload_thread

        self.async_upload_enabled = enabled
        self.shm_as_upload_tmp = shm_as_tmp
        if enabled:
            async_upload_queue = mp.Manager().Queue()
            async_upload_thread = launch_async_upload_thread(async_upload_queue)
            self.async_upload_queue = async_upload_queue
            self.async_upload_thread = async_upload_thread
        yield
        if enabled:
            async_upload_queue.put(None)
            async_upload_thread.join()
            if async_upload_thread.exitcode != 0:
                raise RuntimeError('async upload thread failed with exit code {}'.format(
                    async_upload_thread.exitcode))

        self.async_upload_queue = old_async_upload_queue
        self.async_upload_thread = old_async_upload_thread
        self.async_upload_enabled = old_enabled
        self.shm_as_upload_tmp = old_shm

    def get_file_size(self, fname):
        info = self.get_remote_cache(fname)
        if len(info) == 0:
            return get_file_size(fname)
        remote_file = op.join(info['remote'], info['sub_name'])
        cache_file = op.join(info['cache'], info['sub_name'])
        if self.is_cache_file_ready(cache_file):
            return get_file_size(cache_file)
        return self.account2cloud[info['storage_account']].query_info(
            remote_file)['size_in_bytes']

    def get_creation_time(self, fname):
        info = self.get_remote_cache(fname)
        if len(info) == 0:
            return os.path.getmtime(fname)
        remote_file = op.join(info['remote'], info['sub_name'])
        return self.account2cloud[info['storage_account']].query_info(
            remote_file)['creation_time']

    def clear_cache(self, folder):
        if isinstance(folder, str):
            folders = [folder]
        else:
            folders = folder
        logger.info('clearing {}'.format(','.join(folders)))
        for f in folders:
            info = self.get_remote_cache(f)
            t = op.join(info['cache'], info['sub_name'])
            ensure_remove_dir(t)
            ensure_remove_file(t)

    def upload_from_cache(self, fname):
        info = self.get_remote_cache(fname)
        cloud = self.account2cloud[info['storage_account']]
        remote_file = op.join(info['remote'], info['sub_name'])
        cache_file = op.join(info['cache'], info['sub_name'])
        cloud.upload_file(cache_file, remote_file)

    def upload(self, cache, fname):
        info = self.get_remote_cache(fname)
        cloud = self.account2cloud[info['storage_account']]
        remote_file = op.join(info['remote'], info['sub_name'])
        cloud.upload_file(cache, remote_file)

    def read_from_cloud(self, fname, offset, length):
        info = self.get_remote_cache(fname)
        cloud = self.account2cloud[info['storage_account']]
        remote_file = op.join(info['remote'], info['sub_name'])
        return cloud.read(remote_file, offset, length)

    def break_lease(self, fname):
        info = self.get_remote_cache(fname)
        cloud = self.account2cloud[info['storage_account']]
        remote_file = op.join(info['remote'], info['sub_name'])
        cloud.break_lease(remote_file)

    def upload_from_remote(self, fname, from_fuse):
        info = self.get_remote_cache(fname)
        remote_info = from_fuse.get_remote_cache(fname)
        cloud = self.account2cloud[info['storage_account']]
        cloud.upload(op.join(info['remote'], info['sub_name']),
                     op.join(remote_info['remote'], remote_info['sub_name']),
                     from_blob=from_fuse.account2cloud[remote_info['storage_account']])

    def ensure_cache(self, fname_or_fs,
                     touch_cache_if_exist=False):
        if isinstance(fname_or_fs, (tuple, list)) and len(fname_or_fs) == 1:
            fname_or_fs = fname_or_fs[0]
        if isinstance(fname_or_fs, (tuple, list)):
            fname_or_fs = list(set(fname_or_fs))
            if len(fname_or_fs) == 1:
                fname_or_fs = fname_or_fs[0]
        if isinstance(fname_or_fs, str):
            fname = fname_or_fs
            info = self.get_remote_cache(fname)
            if len(info) == 0:
                return
            remote_file = op.join(info['remote'], info['sub_name'])
            cache_file = op.join(info['cache'], info['sub_name'])
            cloud = self.account2cloud[info['storage_account']]
            self.ensure_remote_to_cache(
                remote_file, cache_file, cloud,
                touch_cache_if_exist=touch_cache_if_exist,
                cache_lock=info['cache_lock'],
            )
        elif isinstance(fname_or_fs, (tuple, list)) and len(fname_or_fs) > 0:
            # for multiple files, we can download them at the same time rather
            # than one by one
            fnames = fname_or_fs
            remote_cache_infos = [self.get_remote_cache(f) for f in fnames]
            remote_cache_infos = [info for info in remote_cache_infos if len(info) > 0]
            if touch_cache_if_exist:
                exist_infos = [info for info in remote_cache_infos if op.isfile(op.join(info['cache'], info['sub_name']))]
                for info in exist_infos:
                    os.utime(op.join(info['cache'], info['sub_name']), None)
            remote_cache_infos = [info for info in remote_cache_infos if
                                  not self.is_cache_file_ready(op.join(info['cache'], info['sub_name']))]
            if len(remote_cache_infos) > 0:
                from .common import acquire_lock
                with acquire_lock(get_azcopy_lock_file()) as lock_fd:
                    # we need to check again
                    remote_cache_infos = [info for info in remote_cache_infos if
                        not self.is_cache_file_ready(op.join(info['cache'], info['sub_name']))]
                    for info in remote_cache_infos:
                        self.clear_cache_related(op.join(info['cache'], info['sub_name']))
                    from .common import list_to_dict
                    config_infos = [(id(info['config_info']), info) for info in remote_cache_infos]
                    config2infos = list_to_dict(config_infos, 0)
                    for infos in config2infos.values():
                        sns = [i['path_relative_to_config'] for i in infos]
                        config = infos[0]['config_info']
                        rd = config['remote']
                        cd = config['cache']
                        cloud = self.account2cloud[config['storage_account']]

                        if len(sns) > 0:
                            file_list = '/tmp/{}'.format(hash_sha1(pformat(sns)))
                            write_to_file('\n'.join(sns), file_list)
                            cloud = self.account2cloud[infos[0]['storage_account']]
                            cloud.az_download(
                                rd,
                                cd,
                                is_folder=True,
                                file_list=file_list,
                                tmp_first=False,
                                sync=False,
                                retry=5,
                            )
                            for s in sns:
                                if not op.isfile(op.join(cd, s)):
                                    logging.error((op.join(cd, s), file_list))

    def open(self, fname, mode):
        info = self.get_remote_cache(fname)
        if len(info) == 0:
            if mode in ['r', 'rb']:
                import io
                return io.open(fname, mode)
            else:
                assert mode in ['w', 'wb'], f'{mode} is not supported'
                return robust_open_to_write(fname, mode)
        if mode in ['r', 'rb']:
            return self.open_to_read(info, mode)
        else:
            # append is not support
            assert mode in ['w', 'wb'], f'{mode} is not supported'
            return self.open_to_write(info, mode)

    @contextlib.contextmanager
    def open_to_write(self, info, mode):
        remote_file = op.join(info['remote'], info['sub_name'])
        if self.shm_as_upload_tmp:
            import copy
            info = copy.deepcopy(info)
            assert info['cache'][0] == '/'
            if not info['cache'].startswith('/dev/shm'):
                info['cache'] = op.join('/dev/shm', info['cache'][1:])
            if not info['config_info']['cache'].startswith('/dev/shm'):
                info['config_info']['cache'] = op.join('/dev/shm', info['config_info']['cache'][1:])
            info['clear_cache_after_upload'] = True
        cache_file = op.join(info['cache'], info['sub_name'])
        tmp_cache_file = cache_file + '.tmpwrite'
        ensure_directory(op.dirname(tmp_cache_file))
        with io.open(tmp_cache_file, mode) as fp:
            yield fp
        os.rename(tmp_cache_file, cache_file)
        if self.async_upload_enabled:
            self.async_upload_queue.put(info)
        else:
            cloud = self.account2cloud[info['storage_account']]
            self.cache_to_remote(cache_file, remote_file, cloud)
            if self.remove_cache_after_upload:
                ensure_remove_file(cache_file)

    def open_to_read(self, info, mode):
        start = time.time()
        remote_file = op.join(info['remote'], info['sub_name'])
        cache_file = op.join(info['cache'], info['sub_name'])
        cloud = self.account2cloud[info['storage_account']]
        self.ensure_remote_to_cache(remote_file, cache_file, cloud,
                                    cache_lock=info.get('cache_lock'))
        after_to_cache = time.time()
        wait_if_zero_file_size(cache_file)
        ret = io.open(cache_file, mode)
        after_open = time.time()
        if after_open - start > 10:
            logging.warning('download {} to {}: {}; open cache: {}'.format(
                remote_file, cache_file, after_to_cache - start, after_open - after_to_cache,
            ))
        return ret

    def ensure_del_cache(self, fname):
        info = self.get_remote_cache(fname)
        if len(info) == 0:
            return
        cache_file = op.join(info['cache'], info['sub_name'])
        #if has_handle(cache_file):
            #logging.warning('{} is used by some process'.format(
                #cache_file))
            #return False
        ensure_remove_file(cache_file)
        #return True

    def ensure_invoke_garbage_collect(self):
        # if the current occupied files take more than 0.5. Then, remove the
        # oldest files
        if self.invoked_collector:
            return
        self.invoked_collector = True
        local_caches = [c['cache'] for c in self.config]
        local_caches = list(set(local_caches))
        # 1TB
        limit_max = get_azfuse_env(
            'CLOUD_FUSE_GARBAGE_COLLECTION_MAX',
            '{}'.format(0.5),
        )
        limit = float(limit_max)
        t = mp.Process(
            target=garbage_collection_for_cloud_fuse_loop,
            args=(local_caches, limit),
            daemon=True,
        )
        t.start()
        self.garbage_collector = t

    #--------------------------------------------------------------
    def acquireLock(self, x=None):
        # all files share one lock
        return acquire_azcopy_lock()

    def get_remote_cache(self, fname):
        fname = op.abspath(fname)
        infos = [
            c for c in self.config if
            fname.startswith(c['local'] + '/') or fname == c['local']
        ]
        if len(infos) == 0:
            return {}
        info = infos[0]
        local_folder = info['local']
        remote_folder, cache_folder = info['remote'], info['cache']
        sub_name = fname[len(local_folder) + 1:]
        d = op.dirname(sub_name)
        sub = op.basename(sub_name)
        return {
            'remote': op.join(remote_folder, d),
            'cache': op.join(cache_folder, d),
            'sub_name': sub,
            'storage_account': info['storage_account'],
            'cache_lock': info.get('cache_lock', False),
            'config_info': info,
            'path_relative_to_config': sub_name,
        }

    def remote_to_cache(self, remote_file, cache_file, cloud, cache_lock):
        if self.is_cache_file_ready(cache_file):
            return
        lock_fd = self.acquireLock()
        if cache_lock:
            cache_lock_pt = acquireLock(cache_file + '.lockfile')
        if not op.isfile(cache_file):
            cloud.az_download(
                remote_file, cache_file,
                #sync=False,
                #is_folder=False,
                retry=5,
            )
        if cache_lock:
            releaseLock(cache_lock_pt)
            #ensure_remove_file(cache_file + '.lockfile')
        releaseLock(lock_fd)

    def cache_to_remote(self, cache_file, remote_file, cloud):
        lock_fd = self.acquireLock()
        assert op.isfile(cache_file)
        cloud.upload(cache_file, remote_file)
        releaseLock(lock_fd)

    def ensure_remote_to_cache(self, remote_file, cache_file, cloud,
                               touch_cache_if_exist=False,
                               cache_lock=False,
                               ):
        if self.is_cache_file_ready(cache_file):
            if touch_cache_if_exist:
                os.utime(cache_file, None)
        else:
            self.remote_to_cache(remote_file, cache_file, cloud, cache_lock)

    def set_access_tier(self, fname, tier):
        info = self.get_remote_cache(fname)
        if len(info) == 0:
            return
        cloud = self.account2cloud[info['storage_account']]
        cloud.set_access_tier(op.join(info['remote'], info['sub_name']), tier)

    def list(self, folder, recursive=False, return_info=False):
        info = self.get_remote_cache(folder)
        if len(info) == 0:
            return glob.glob(op.join(folder, '*'), recursive=recursive)
        else:
            cloud = self.account2cloud[info['storage_account']]
            if info['sub_name'] == '':
                remote_folder = info['remote']
            else:
                remote_folder = op.join(info['remote'], info['sub_name'])
            remote_folder = remote_folder.rstrip('/')
            if not return_info:
                if remote_folder == '':
                    ret = list(cloud.list_blob_names())
                else:
                    ret = list(cloud.list_blob_names(remote_folder + '/', recursive=recursive))
                if not recursive:
                    result = []
                    for r in ret:
                        rest = r[len(remote_folder):]
                        while rest.startswith('/'):
                            rest = rest[1:]
                        idx = rest.find('/')
                        if idx != -1:
                            rest = rest[:idx]
                        result.append(rest)
                    result = list(set(result))
                    result = sorted(result)
                    return [op.join(folder, r) for r in result]
                else:
                    return [r.replace(remote_folder, folder) for r in ret]
            else:
                prefix = None
                if remote_folder:
                    prefix = remote_folder + '/'
                ret = list(cloud.iter_blob_info(prefix, recursive=recursive))
                if len(ret) == 0:
                    ret.append(cloud.query_info(remote_folder))
                if not recursive:
                    result = {}
                    for r in ret:
                        rest = r['name'][len(remote_folder):]
                        while rest.startswith('/'):
                            rest = rest[1:]
                        idx = rest.find('/')
                        if idx != -1:
                            rest = rest[:idx]
                        r['name'] = rest
                        if r['name'] in result:
                            if 'size_in_bytes' not in result[r['name']]:
                                result[r['name']]['size_in_bytes'] = 0
                            if 'creation_time' not in result[r['name']]:
                                import pytz
                                result[r['name']]['creation_time'] = datetime.datetime.now(pytz.utc)
                            result[r['name']]['size_in_bytes'] += r['size_in_bytes']
                            result[r['name']]['creation_time'] = min(
                                result[r['name']]['creation_time'],
                                r['creation_time']
                            )
                        else:
                            result[r['name']] = r
                    result = result.values()
                    result = sorted(result, key=lambda r: r['name'])
                    for r in result:
                        r['name'] = op.join(folder, r['name'])
                    return result
                else:
                    for r in ret:
                        r['name'] = r['name'].replace(remote_folder, folder)
                    return ret

def get_storage_package_version():
    import pkg_resources
    azure_storage_blob_version = pkg_resources.get_distribution('azure-storage-blob').version
    # e.g. 2.1.0
    return azure_storage_blob_version

class CloudStorage(object):
    def __init__(self, config=None):
        if config is None:
            config_file = 'aux_data/configs/azure_blob_account.yaml'
            config = load_from_yaml_file(config_file)
        account_name = config['account_name']
        account_key = config.get('account_key')
        self.sas_token = config.get('sas_token')
        self.use_default_azure_cred = config.get('use_default_azure_cred')
        self.container_name = config['container_name']
        self.account_name = account_name
        self.account_key = account_key
        self._block_blob_service = None
        self._is_new_package = None
        self._curr_pid = None

    @property
    def is_new_package(self):
        if self._is_new_package is None:
            self._is_new_package = get_storage_package_version().startswith('12.')
        return self._is_new_package

    def __repr__(self):
        return 'CloudStorage(account={}, container={})'.format(
            self.account_name,
            self.container_name,
        )

    @property
    def block_blob_service(self):
        if self._block_blob_service is None or os.getpid() != self._curr_pid:
            # it seems like there is some race condition in multi-process env
            self._curr_pid = os.getpid()
            if self.is_new_package:
                from azure.storage.blob import BlobServiceClient
                if self.sas_token:
                    self._block_blob_service = BlobServiceClient(
                        account_url='https://{}.blob.core.windows.net'.format(self.account_name),
                        credential=self.sas_token)
                elif self.account_key:
                    self._block_blob_service = BlobServiceClient(
                        account_url='https://{}.blob.core.windows.net/'.format(self.account_name),
                        credential={'account_name': self.account_name, 'account_key': self.account_key})
                elif self.use_default_azure_cred:
                    from azure.identity import DefaultAzureCredential
                    credential = DefaultAzureCredential()
                    self._block_blob_service = BlobServiceClient(
                        account_url='https://{}.blob.core.windows.net/'.format(self.account_name),
                        credential=credential)
                else:
                    self._block_blob_service = BlobServiceClient(
                        account_url='https://{}.blob.core.windows.net/'.format(self.account_name),
                        )
            else:
                from azure.storage.blob import BlockBlobService
                self._block_blob_service = BlockBlobService(
                        account_name=self.account_name,
                        account_key=self.account_key,
                        sas_token=self.sas_token)
        return self._block_blob_service

    @property
    def container_client(self):
        assert self.is_new_package
        return self.block_blob_service.get_container_client(self.container_name)

    def list_blob_names(self, prefix=None, recursive=False, creation_time_larger_than=None):
        for info in self.iter_blob_info(prefix, creation_time_larger_than, recursive=recursive):
            yield info['name']

    def set_access_tier(self, fname, tier):
        blob_client = self.container_client.get_blob_client(fname)
        blob_client.set_standard_blob_tier(tier)

    def break_lease(self, fname):
        blob_client = self.container_client.get_blob_client(fname)
        from azure.storage.blob import BlobLeaseClient
        lease_client = BlobLeaseClient(blob_client)
        lease_client.break_lease()

    def du_max_depth_1(self, path, deleted=False):
        from collections import defaultdict
        ret = defaultdict(int)
        for info in tqdm(self.iter_blob_info(path, deleted=deleted)):
            assert info['name'].startswith(path)
            root = info['name'][len(path):]
            if root.startswith('/'):
                root = root[1:]
            root = root.split('/')[0]
            ret[root] += info['size_in_bytes']
        return ret

    def rm_prefix(self, prefix):
        all_path = self.list_blob_names(prefix)
        for p in all_path:
            logger.info('deleting {}'.format(p))
            self.rm(p)

    def rm(self, path):
        if not self.is_new_package:
            self.block_blob_service.delete_blob(self.container_name, path)
        else:
            blob = self.container_client.get_blob_client(path)
            blob.delete_blob()

    def iter_blob_info(self, prefix=None,
                       creation_time_larger_than=None,
                       deleted=False,
                       recursive=False,
                       ):
        def valid(b):
            c1 = creation_time_larger_than is None or b.creation_time.timestamp() > creation_time_larger_than.timestamp()
            c2 = b.name.startswith(prefix) if prefix else True
            return c1 and c2 and (not deleted or b.deleted)
        for b in self.container_client.walk_blobs(
            name_starts_with=prefix,
            include='deleted' if deleted else None,
        ):
            from azure.core.paging import ItemPaged
            if isinstance(b, ItemPaged):
                if recursive:
                    yield from self.iter_blob_info(prefix=b.name,
                                                   creation_time_larger_than=creation_time_larger_than,
                                                   deleted=deleted,
                                                   recursive=recursive)
                else:
                    yield {
                        'name': b.name,
                    }
            elif valid(b):
                ret = {
                    'name': b.name,
                    'size_in_bytes': b.size,
                    'creation_time': b.creation_time,
                    'last_modified': b.last_modified,
                    'blob_tier': self.get_access_tier(b),
                    'deleted': b.deleted,
                    'lease_status': b.lease['status'],
                }
                yield ret

    def get_access_tier(self, blob_properties):
        blob_tier = blob_properties.blob_tier
        if blob_tier == 'Archive' and blob_properties.archive_status:
            if 'pending' in blob_properties.archive_status:
                blob_tier = blob_properties.archive_status
        return blob_tier

    def list_blob_info(self, prefix=None, creation_time_larger_than=None):
        return list(self.iter_blob_info(prefix, creation_time_larger_than))

    def get_url(self, blob_name):
        if self.is_new_package:
            from azure.storage.blob import generate_blob_sas, BlobSasPermissions
            # BlobClient.url can also give the url, but the sas token is the
            # default token which has more than expected permissions
            sas = generate_blob_sas(
                account_name=self.account_name,
                container_name=self.container_name,
                blob_name=blob_name,
                account_key=self.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.datetime.now() + datetime.timedelta(days=30))
            url = 'https://'+self.account_name+'.blob.core.windows.net/'+self.container_name+'/'+blob_name
        else:
            from azure.storage.blob.models import BlobPermissions
            permission = BlobPermissions(read=True)
            expiry = datetime.datetime.now() + datetime.timedelta(days=30)
            sas = self.block_blob_service.generate_blob_shared_access_signature(
                self.container_name, blob_name,
                permission=permission,
                expiry=expiry,
            )
            url = self.block_blob_service.make_blob_url(self.container_name,
                                                        blob_name)
        return '{}?{}'.format(url, sas)

    def upload_stream(self, s, path, force=False, lease=None):
        if self.is_new_package:
            blob_client = self.container_client.get_blob_client(path)
            blob_client.upload_blob(s, lease=lease, overwrite=force)
        else:
            if not force and self.block_blob_service.exists(self.container_name,
                    path):
                return self.block_blob_service.make_blob_url(
                        self.container_name,
                        path)
            else:
                if type(s) is bytes:
                    self.block_blob_service.create_blob_from_bytes(
                            self.container_name,
                            path,
                            s)
                else:
                    self.block_blob_service.create_blob_from_stream(
                            self.container_name,
                            path,
                            s)
                return self.block_blob_service.make_blob_url(
                        self.container_name,
                        path)

    def upload_folder(self, folder, target_prefix):
        def remove_tailing(x):
            if x.endswith('/') or x.endswith('\\'):
                x = x[:-1]
            return x
        folder = remove_tailing(folder)
        target_prefix = remove_tailing(target_prefix)
        for root, dirs, files in os.walk(folder):
            for f in files:
                src_file = op.join(root, f)
                assert src_file.startswith(folder)
                target_file = src_file.replace(folder, target_prefix)
                self.upload_file(src_file, target_file)
            for d in dirs:
                self.upload_folder(op.join(root, d),
                        op.join(target_prefix, d))

    def upload_file(self, src_file, target_file):
        self.upload(src_file, target_file)

    @deprecated('use az_upload2()')
    def az_upload(self, src_dir, dest_dir):
        # this is using the old version of azcopy. prefer to use az_upload2
        dest_url = op.join('https://{}.blob.core.windows.net'.format(self.account_name),
                self.container_name, dest_dir)
        if self.account_key:
            azcopy_upload(src_dir, dest_url, self.account_key)
        else:
            raise Exception

    @deprecated('use upload()')
    def az_sync(self, src_dir, dest_dir):
        self.upload(src_dir, dest_dir)

    def upload(self, src_dir, dest_dir, from_blob=None, file_list=None):
        if from_blob is None:
            self.upload_from_local(src_dir, dest_dir, file_list=file_list)
        else:
            assert file_list is None
            self.upload_from_another(src_dir, dest_dir, from_blob)

    def upload_from_another(self, src_dir, dest_dir, from_blob):
        assert self.sas_token
        cmd = []
        cmd.append(get_azcopy())
        if from_blob.dir_exists(src_dir) and not self.dir_exists(dest_dir):
            # in this case, azcopy will copy the local folder under the
            # destination folder, and thus we have to use the folder of
            # dest_dir as the dest_dir.
            assert op.basename(src_dir) == op.basename(dest_dir)
            dest_dir = op.dirname(dest_dir)
            cmd.append('cp')
        else:
            if self.exists(dest_dir):
                cmd.append('sync')
            else:
                cmd.append('cp')
        cmd.append('--put-md5')
        url = 'https://{}.blob.core.windows.net'.format(from_blob.account_name)
        url = op.join(url, from_blob.container_name, src_dir)
        assert self.sas_token.startswith('?')
        from_url = url
        url = url + from_blob.sas_token
        cmd.append(url)

        url = 'https://{}.blob.core.windows.net'.format(self.account_name)
        if dest_dir.startswith('/'):
            dest_dir = dest_dir[1:]
        url = op.join(url, self.container_name, dest_dir)
        assert self.sas_token.startswith('?')
        data_url = url
        url = url + self.sas_token
        cmd.append(url)

        if from_blob.dir_exists(src_dir):
            cmd.append('--recursive')
        if from_url == data_url:
            logger.info('no need to sync data as url is exactly the same')
            return data_url, url
        if int(get_azfuse_env('AZCOPY_NO_LOG', '0')):
            import subprocess
            stdout = subprocess.DEVNULL
            silent = True
        else:
            stdout = None
            silent = False
        cmd_run(cmd, stdout=stdout, stderr=sp.PIPE, silent=silent)
        return data_url, url

    def upload_from_local(self, src_dir, dest_dir, file_list=None):
        assert self.sas_token
        cmd = []
        cmd.append(get_azcopy())
        if file_list is not None or (op.isdir(src_dir) and not
                                     self.dir_exists(dest_dir)):
            # in this case, azcopy will copy the local folder under the
            # destination folder, and thus we have to use the folder of
            # dest_dir as the dest_dir.
            assert op.basename(src_dir) == op.basename(dest_dir)
            if dest_dir.endswith('/'):
                dest_dir = dest_dir[:-1]
            dest_dir = op.dirname(dest_dir)
            cmd.append('cp')
        else:
            # logger.info(dest_dir)
            if file_list is None and self.exists(dest_dir):
                cmd.append('sync')
            else:
                cmd.append('cp')
        cmd.append('--put-md5')
        cmd.append(op.realpath(src_dir))
        url = 'https://{}.blob.core.windows.net'.format(self.account_name)
        if dest_dir.startswith('/'):
            dest_dir = dest_dir[1:]
        url = op.join(url, self.container_name, dest_dir)
        assert self.sas_token.startswith('?')
        data_url = url
        url = url + self.sas_token
        cmd.append(url)
        if op.isdir(src_dir):
            cmd.append('--recursive')
        if file_list:
            cmd.append('--list-of-files')
            cmd.append(file_list)
        if int(get_azfuse_env('AZCOPY_NO_LOG', '1')):
            import subprocess
            stdout = subprocess.DEVNULL
            silent = True
        else:
            stdout = None
            silent = False
        cmd_run(cmd, stdout=stdout, silent=silent)
        return data_url, url

    @deprecated('use upload')
    def az_upload2(self, src_dir, dest_dir, sync=False):
        return self.upload(src_dir, dest_dir)

    def query_info(self, path):
        if not self.is_new_package:
            try:
                p = self.block_blob_service.get_blob_properties(self.container_name, path)
            except:
                logger.info('{}: {}'.format(self.container_name, path))
                raise
            result = {
                'size_in_bytes': p.properties.content_length,
                'creation_time': p.properties.creation_time,
                'name': path,
            }
        else:
            try:
                blob_client = self.container_client.get_blob_client(path)
            except:
                logger.info('{}: {}'.format(self.container_name, path))
                raise
            blob_properties = blob_client.get_blob_properties()
            blob_tier = self.get_access_tier(blob_properties)
            result = {
                'size_in_bytes': blob_properties.size,
                'creation_time': blob_properties.creation_time,
                'blob_tier': blob_tier,
                'name': path,
                'lease_status': blob_properties.lease['status'],
            }
        return result

    def az_download_all(self, remote_path, local_path):
        all_blob_name = list(self.list_blob_names(remote_path))
        all_blob_name = get_leaf_names(all_blob_name)
        for blob_name in all_blob_name:
            target_file = blob_name.replace(remote_path, local_path)
            if not op.isfile(target_file):
                self.az_download(blob_name, target_file,
                        sync=False)

    def is_folder(self, remote_path):
        remote_path = op.normpath(remote_path)
        def is_folder_once():
            is_folder = False
            for x in self.list_blob_names(remote_path + '/'):
                is_folder = True
                break
            return is_folder
        return limited_retry_agent(
            10,
            is_folder_once,
        )

    def az_download_each(self, remote_path, local_path):
        # if it is a folder, we will download each file individually
        if remote_path.startswith('/'):
            remote_path = remote_path[1:]
        if remote_path.endswith('/'):
            remote_path = remote_path[:-1]
        is_folder = self.is_folder(remote_path)
        if is_folder:
            all_remote_file = self.list_blob_names(remote_path + '/')
            all_local_file = [op.join(local_path, r[len(remote_path) + 1:])
                              for r in all_remote_file]
        else:
            all_remote_file = [remote_path]
            all_local_file = [local_path]
        for r, l in zip(all_remote_file, all_local_file):
            self.az_download(r, l, sync=True)

    def az_download(self,
                    remote_path,
                    local_path,
                    sync=True,
                    is_folder=None,
                    tmp_first=True,
                    file_list=None,
                    retry=30,
                    ):
        # retry = 5 is good enough for premium storage account, but it seems
        # like it is not for standard. Here we change it to 30 and see if it
        # works well
        def retry_pre_func():
            os.environ['AZFUSE_AZCOPY_NO_LOG'] = '0'
        limited_retry_agent(retry, self.az_download_once,
                            remote_path, local_path, sync,
                            is_folder, tmp_first, file_list,
                            __retry_pre_func=retry_pre_func,
                            )

    @contextlib.contextmanager
    def acquire_lease(self, path, metadata=None):
        from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
        blob_client = self.container_client.get_blob_client(path)
        metadata = metadata or {}
        while True:
            try:
                lease = blob_client.acquire_lease()
                blob_client.set_blob_metadata(metadata, lease=lease)
                break
            except ResourceNotFoundError:
                from .common import try_once
                try_once(blob_client.upload_blob)(b'', overwrite=True)
                # blob_client.upload_blob(b'', overwrite=True)
            except ResourceExistsError:
                logger.info(f'there is a process leasing {path}; waiting')
                time.sleep(10)
        yield lease
        lease.release()

    def try_acquire_lease(self, path, metadata=None):
        from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
        blob_client = self.container_client.get_blob_client(path)
        metadata = metadata or {}
        try:
            lease = blob_client.acquire_lease(lease_duration=60)
            if metadata:
                blob_client.set_blob_metadata(metadata, lease=lease)
            return lease
        except ResourceNotFoundError:
            logger.info('lock is not available; waiting for creating')
            return
        except ResourceExistsError:
            logger.info(f'there is a process leasing {path}; waiting')
            return

    @contextlib.contextmanager
    def acquire_lease_renew(self, path, metadata=None, sleep=10):
        from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
        blob_client = self.container_client.get_blob_client(path)
        metadata = metadata or {}
        while True:
            try:
                lease = blob_client.acquire_lease(lease_duration=60)
                blob_client.set_blob_metadata(metadata, lease=lease)
                break
            except ResourceNotFoundError:
                logger.info('lock is not available; waiting for creating')
                time.sleep(sleep)
            except ResourceExistsError:
                logger.info(f'there is a process leasing {path}; waiting')
                time.sleep(sleep)
        import threading
        continue_leasing = True
        def renew_lease(lease):
            while continue_leasing:
                lease.renew()
                for _ in range(10):
                    if continue_leasing:
                        time.sleep(2)
        th = threading.Thread(target=renew_lease, args=(lease,), daemon=True)
        th.start()
        yield lease
        continue_leasing = False
        th.join()
        lease.release()

    def az_download_once(self,
                    remote_path,
                    local_path,
                    sync=True,
                    is_folder=None,
                    tmp_first=True,
                    file_list=None,
                    ):
        if remote_path.startswith('/'):
            remote_path = remote_path[1:]
        if remote_path.endswith('/'):
            remote_path = remote_path[:-1]
        if is_folder is None:
            is_folder = self.is_folder(remote_path)
        if sync and tmp_first:
            tmp_first = False
        if is_folder:
            if not sync and op.isdir(local_path) and tmp_first:
                if len(os.listdir(local_path)) > 0:
                    logging.error('ignore to download from {} to {}'
                                  ' since destination is not empty'.format(
                                      remote_path,
                                      local_path,
                                  ))
                    return
                ensure_remove_dir(local_path)
        else:
            if sync:
                if tmp_first:
                    sync = False
                elif not op.isfile(local_path):
                    sync = False
        ensure_directory(op.dirname(local_path))
        origin_local_path = local_path
        if tmp_first:
            local_path = local_path + '.tmp'
        ensure_directory(op.dirname(local_path))
        cmd = []
        cmd.append(get_azcopy())
        if sync:
            cmd.append('sync')
        else:
            cmd.append('cp')
        url = 'https://{}.blob.core.windows.net'.format(self.account_name)
        url = '/'.join([url, self.container_name, remote_path])
        data_url = url
        if self.sas_token:
            assert self.sas_token.startswith('?')
            url = url + self.sas_token
        cmd.append(url)
        if file_list:
            # requirements from file_list
            assert (op.basename(local_path) == op.basename(remote_path) or 
                remote_path == '' and op.basename(local_path) == self.container_name), (local_path, remote_path)
            cmd.append(op.dirname(op.realpath(local_path)))
        else:
            cmd.append(op.realpath(local_path))
        if is_folder:
            cmd.append('--recursive')
            if sync:
                # azcopy's requirement
                ensure_directory(local_path)
        else:
            ensure_directory(op.dirname(local_path))
        if file_list:
            cmd.append('--list-of-files')
            cmd.append(file_list)
        # too much logs. ignore them
        if int(get_azfuse_env('AZCOPY_NO_LOG', '0')):
            import subprocess
            stdout = subprocess.DEVNULL
        else:
            stdout = None
        try:
            cmd_run(cmd, stdout=stdout)
        except:
            logging.warning(pformat(cmd))
            raise
        if tmp_first:
            if not op.isfile(local_path) and not op.isdir(local_path):
                logging.error('{} not exists; {}/{}'.format(
                    local_path, origin_local_path, op.isfile(origin_local_path)))
            os.rename(local_path, origin_local_path)
        return data_url, url

    def download_to_path(self, blob_name, local_path,
            max_connections=2):
        dir_path = op.dirname(local_path)
        if op.isfile(dir_path) and get_file_size(dir_path) == 0:
            os.remove(dir_path)
        ensure_directory(dir_path)
        tmp_local_path = local_path + '.tmp'
        with open(tmp_local_path, 'wb') as f:
            f.write(self.readall(blob_name, max_concurrency=max_connections))
        os.rename(tmp_local_path, local_path)

    def download_to_stream(self, blob_name, s, max_connections=2):
        pbar = {}
        def progress_callback(curr, total):
            if len(pbar) == 0:
                pbar['tqdm'] = tqdm(total=total, unit_scale=True)
                pbar['last'] = 0
                pbar['count'] = 0
            pbar['count'] += 1
            if pbar['count'] > 1:
                pbar['tqdm'].update(curr - pbar['last'])
                pbar['last'] = curr
                pbar['count'] = 0
        self.block_blob_service.get_blob_to_stream(self.container_name,
                                                   blob_name,
                                                   s,
                                                   max_connections=max_connections,
                                                   progress_callback=progress_callback,
                                                   )
    def readall(self, path, max_concurrency=1):
        blob_client = self.container_client.get_blob_client(path)
        return blob_client.download_blob(max_concurrency=max_concurrency).readall()

    def read(self, path, offset=None, length=None):
        blob_client = self.container_client.get_blob_client(path)
        return limited_retry_agent(-1, lambda: blob_client.download_blob(offset=offset, length=length).readall())

    def get_metadata(self, path):
        blob_client = self.container_client.get_blob_client(path)
        return blob_client.get_blob_properties()['metadata']

    def set_metadata(self, metadata, path, **kwargs):
        blob_client = self.container_client.get_blob_client(path)
        return blob_client.set_blob_metadata(metadata, **kwargs)

    def exists(self, path):
        return self.file_exists(path) or self.dir_exists(path)

    def file_exists(self, path):
        if self.is_new_package:
            result = self.container_client.get_blob_client(path).exists()
        else:
            result = limited_retry_agent(
                5,
                self.block_blob_service.exists,
                self.container_name,
                path
            )
        return result

    def dir_exists(self, dir_path):
        return self.is_folder(dir_path)

    @deprecated('no longer used')
    def blob_download_qdoutput(
        self,
        src_path,
        target_folder,
        latest_only=True,
        creation_time_larger_than=None,
        too_large_limit_in_gb=None,
        ignore_base_fname_patterns=None,
        dry_run=False,
    ):
        def is_in_snapshot(b):
            parts = list(b.split('/'))
            return 'snapshot' in parts and parts.index('snapshot') < len(parts) - 1
        all_blob_name = list(self.list_blob_names(
            src_path,
            creation_time_larger_than))
        # remove snapshot/model_iter/abc.bin
        clean = []
        for b in all_blob_name:
            parts = list(b.split('/'))
            if 'snapshot' in parts and \
                    parts.index('snapshot') < len(parts) - 2:
                continue
            else:
                clean.append(b)
        all_blob_name = clean

        all_blob_name = get_leaf_names(all_blob_name)
        in_snapshot_blobs = [b for b in all_blob_name if is_in_snapshot(b)]
        not_in_snapshot_blobs = [b for b in all_blob_name if not is_in_snapshot(b)]
        try:
            not_in_snapshot_blobs.remove(src_path)
        except:
            pass
        try:
            not_in_snapshot_blobs.remove(src_path + '/snapshot')
        except:
            pass
        need_download_blobs = []
        need_download_blobs.extend(not_in_snapshot_blobs)
        iters = [parse_iteration(f) for f in in_snapshot_blobs]
        if len(iters) > 0 and latest_only:
            max_iters = max(iters)
            need_download_blobs.extend([f for f, i in zip(in_snapshot_blobs, iters) if i ==
                    max_iters or f.endswith('.report')])
        need_download_blobs.extend([f for f, i in zip(in_snapshot_blobs, iters) if
                i == -2])
        to_remove = []
        for i, b1 in enumerate(need_download_blobs):
            for b2 in need_download_blobs:
                if b1 != b2 and b2.startswith(b1) and b2.startswith(b1 + '/'):
                    to_remove.append(b1)
                    break
        for t in to_remove:
            need_download_blobs.remove(t)
        need_download_blobs = [t for t in need_download_blobs
            if not t.endswith('.tmp')]
        f_target_f = [(f, f.replace(src_path, target_folder)) for f in
            need_download_blobs]
        f_target_f = [(f, target_f) for f, target_f in f_target_f if not op.isfile(target_f)]
        f_target_f = [(f, target_f) for f, target_f in f_target_f if len(f) > 0]

        if ignore_base_fname_patterns is not None:
            import re
            result = []
            for f, target_f in f_target_f:
                if any(re.match(p, f) is not None for p in
                       ignore_base_fname_patterns):
                    logger.info('ignore {} due to reg pattern matching'.format(f))
                else:
                    result.append((f, target_f))
            f_target_f = result

        if too_large_limit_in_gb is not None:
            logger.info('before size filtering = {}'.format(len(f_target_f)))
            f_target_f = [(f, target_f) for f, target_f in f_target_f if
                          self.query_info(f)['size_in_bytes'] / 1024. ** 3 <=
                          too_large_limit_in_gb]
            logger.info('after size filtering = {}'.format(len(f_target_f)))
        for f, target_f in tqdm(f_target_f):
            logger.info('download {} to {}'.format(f, target_f))
            try:
                if not dry_run:
                    self.az_download(f, target_f)
            except:
                pass
                    #self.download_to_path(f, target_f)

