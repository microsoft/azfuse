import io
import sys
import subprocess as sp
from collections import OrderedDict
import yaml
import shutil
import re
import random
import logging
from pprint import pformat
import psutil
import os.path as op
import os
import contextlib
import time



def get_azfuse_env(v, d=None):
    # this is for back-compatibility only
    qd_k = 'QD_' + v
    if qd_k in os.environ:
        return os.environ[qd_k]
    azfuse_k = 'AZFUSE_' + v
    if azfuse_k in os.environ:
        return os.environ[azfuse_k]
    return d

def init_logging():
    ch = logging.StreamHandler(stream=sys.stdout)
    ch.setLevel(logging.INFO)
    logger_fmt = logging.Formatter('%(asctime)s.%(msecs)03d %(process)d:%(filename)s:%(lineno)s %(funcName)s(): %(message)s')
    ch.setFormatter(logger_fmt)

    root = logging.getLogger()
    root.handlers = []
    root.addHandler(ch)
    root.setLevel(logging.INFO)

def create_logger():
    logger = logging.getLogger('azfuse')
    logger.propagate = False

    ch = logging.StreamHandler(stream=sys.stdout)
    # ch.setLevel(logging.WARNING)
    ch.setLevel(logging.INFO)

    logger_fmt = logging.Formatter('%(asctime)s %(process)d:%(filename)s:%(lineno)s %(funcName)10s(): %(message)s')
    ch.setFormatter(logger_fmt)

    logger.handlers = []
    logger.addHandler(ch)
    log_file='~/.azfuse.log'
    log_file = op.expanduser(log_file)
    from logging.handlers import RotatingFileHandler
    fh = RotatingFileHandler(log_file, maxBytes=10*1024*1024*1024, backupCount=5)
    fh.setLevel(logging.INFO)
    fh.setFormatter(logger_fmt)
    logger.addHandler(fh)

    logger.setLevel(logging.INFO)
    return logger

logger = create_logger()

def get_table_print_lines(a_to_bs, all_key):
    if len(a_to_bs) == 0:
        logger.info('no rows')
        return []
    if not all_key:
        all_key = []
        for a_to_b in a_to_bs:
            all_key.extend(a_to_b.keys())
        all_key = sorted(list(set(all_key)))
    all_width = [max([len(str(a_to_b.get(k, ''))) for a_to_b in a_to_bs] +
        [len(k)]) for k in all_key]
    row_format = ' '.join(['{{:{}}}'.format(w) for w in all_width])

    all_line = []
    line = row_format.format(*all_key)
    all_line.append(line.strip())
    for a_to_b in a_to_bs:
        line = row_format.format(*[str(a_to_b.get(k, '')) for k in all_key])
        all_line.append(line)
    return all_line

def print_table(a_to_bs, all_key=None, **kwargs):
    if len(a_to_bs) == 0:
        return
    all_line = get_table_print_lines(a_to_bs, all_key)
    logger.info('\n{}'.format('\n'.join(all_line)))

def get_env_value(keys, default):
    for k in keys:
        if k in os.environ:
            return os.environ[k]
    return default

def get_mpi_local_rank():
    if 'LOCAL_RANK' in os.environ:
        return int(os.environ['LOCAL_RANK'])
    keys = ['OMPI_COMM_WORLD_LOCAL_RANK', 'MPI_LOCALRANKID']
    return int(get_env_value(keys, '0'))

def dict_parse_key(k, with_type):
    if with_type:
        if k[0] == 'i':
            return int(k[1:])
        else:
            return k[1:]
    return k

def dict_get_path_value(d, p, with_type=False):
    ps = p.split('$')
    cur_dict = d
    while True:
        if len(ps) > 0:
            k = dict_parse_key(ps[0], with_type)
            if isinstance(cur_dict, (tuple, list)):
                cur_dict = cur_dict[int(k)]
            else:
                cur_dict = cur_dict[k]
            ps = ps[1:]
        else:
            return cur_dict

def get_all_path(d, with_type=False, leaf_only=True, with_list=True):
    assert not with_type, 'will not support'
    all_path = []

    if isinstance(d, dict):
        for k, v in d.items():
            all_sub_path = get_all_path(
                v, with_type, leaf_only=leaf_only, with_list=with_list)
            all_path.extend([k + '$' + p for p in all_sub_path])
            if not leaf_only or len(all_sub_path) == 0:
                all_path.append(k)
    elif (isinstance(d, tuple) or isinstance(d, list)) and with_list:
        for i, _v in enumerate(d):
            all_sub_path = get_all_path(
                _v, with_type,
                leaf_only=leaf_only,
                with_list=with_list,
            )
            all_path.extend(['{}$'.format(i) + p for p in all_sub_path])
            if not leaf_only or len(all_sub_path) == 0:
                all_path.append('{}'.format(i))
    return all_path

def load_from_yaml_str(s):
    return yaml.load(s, Loader=yaml.UnsafeLoader)

def get_user_name():
    import getpass
    return getpass.getuser()

def exclusive_open_to_read(fname, mode='r'):
    disable_lock = get_azfuse_env('DISABLE_EXCLUSIVE_READ_BY_LOCK', None)
    if disable_lock is not None:
        disable_lock = int(disable_lock)
    if not disable_lock:
        user_name = get_user_name()
        lock_fd = acquireLock(op.join('/tmp',
            '{}_lock_{}'.format(user_name, hash_sha1(fname))))
    #try:
    # in AML, it could fail with Input/Output error. If it fails, we will
    # use azcopy as a fall back solution for reading
    fp = limited_retry_agent(10, io.open, fname, mode)
    if not disable_lock:
        releaseLock(lock_fd)
    return fp

def try_once(func):
    def func_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.info('ignore error \n{}'.format(str(e)))
            print_trace()
    return func_wrapper

def print_trace():
    import traceback
    traceback.print_exc()

@contextlib.contextmanager
def robust_open_to_write(fname, mode):
    tmp = fname + '.tmp'
    ensure_directory(op.dirname(tmp))
    with io.open(tmp, mode) as fp:
        yield fp
    os.rename(tmp, fname)

def query_all_opened_file_in_system():
    fs = []
    for proc in psutil.process_iter():
        for proc in psutil.process_iter():
            try:
                for item in proc.open_files():
                    fs.append(item.path)
            except Exception:
                pass
    return list(set(fs))

@try_once
def ensure_remove_file(d):
    if op.isfile(d) or op.islink(d):
        try:
            os.remove(d)
        except:
            pass

def has_handle(fpath, opened_files=None):
    fpath = op.abspath(op.realpath(fpath))
    if opened_files is None:
        for proc in psutil.process_iter():
            try:
                for item in proc.open_files():
                    if fpath == item.path:
                        return True
            except Exception:
                pass
        return False
    else:
        return fpath in opened_files

def wait_if_zero_file_size(f, t=10):
    if get_file_size(f) == 0:
        time.sleep(t)

@try_once
def get_file_size(f):
    return os.stat(f).st_size

def hash_sha1(s):
    import hashlib
    if type(s) is not str:
        s = pformat(s)
    return hashlib.sha1(s.encode('utf-8')).hexdigest()

@contextlib.contextmanager
def acquire_lock(lock_f):
    import fcntl
    ensure_directory(op.dirname(lock_f))
    locked_file_descriptor = open(lock_f, 'w+')
    fcntl.lockf(locked_file_descriptor, fcntl.LOCK_EX)
    yield locked_file_descriptor
    locked_file_descriptor.close()

def acquireLock(lock_f='/tmp/lockfile.LOCK'):
    import fcntl
    ensure_directory(op.dirname(lock_f))
    locked_file_descriptor = io.open(lock_f, 'w+')
    fcntl.lockf(locked_file_descriptor, fcntl.LOCK_EX)
    return locked_file_descriptor

def releaseLock(locked_file_descriptor):
    locked_file_descriptor.close()

def write_to_file(contxt, file_name, append=False):
    p = os.path.dirname(file_name)
    ensure_directory(p)
    if type(contxt) is str:
        contxt = contxt.encode()
    flag = 'wb'
    if append:
        flag = 'ab'
    with io.open(file_name, flag) as fp:
        fp.write(contxt)

def limited_retry_agent(num, func, *args, **kwargs):
    i = 0
    retry_pre_func = kwargs.pop('__retry_pre_func', None)
    while True:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning('fails with \n{}: tried {}/{}-th time'.format(
                e,
                i + 1,
                num,
            ))
            import time
            print_trace()
            if num > 0 and i == num - 1:
                raise
            i += 1
            if retry_pre_func:
                retry_pre_func()
            t = random.random() * 5
            time.sleep(t)

def list_to_dict(l, idx, keep_one=False):
    result = OrderedDict()
    for x in l:
        if x[idx] not in result:
            result[x[idx]] = []
        y = x[:idx] + x[idx + 1:]
        if not keep_one and len(y) == 1:
            y = y[0]
        result[x[idx]].append(y)
    return result

def find_mount_point(path):
    path = op.abspath(op.realpath(path))
    while not op.ismount(path):
        path = op.dirname(path)
    return path

@try_once
def ensure_remove_dir(d):
    is_dir = op.isdir(d)
    is_link = op.islink(d)
    if is_dir:
        if not is_link:
            shutil.rmtree(d)
        else:
            os.unlink(d)

def dict_update_path_value(d, p, v):
    ps = p.split('$')
    while True:
        if len(ps) == 1:
            d[ps[0]] = v
            break
        else:
            if ps[0] not in d:
                d[ps[0]] = {}
            d = d[ps[0]]
            ps = ps[1:]

def load_from_yaml_file(file_name):
    # do not use QDFile.open as QDFile.open depends on this function
    with exclusive_open_to_read(file_name, 'r') as fp:
    #with open(file_name, 'r') as fp:
        data = load_from_yaml_str(fp)
    while isinstance(data, dict) and '_base_' in data:
        b = op.join(op.dirname(file_name), data['_base_'])
        result = load_from_yaml_file(b)
        assert isinstance(result, dict)
        del data['_base_']
        all_key = get_all_path(data, with_list=False)
        for k in all_key:
            v = dict_get_path_value(data, k)
            dict_update_path_value(result, k, v)
        data = result
    return data

def decode_to_str(x):
    try:
        return x.decode('utf-8')
    except UnicodeDecodeError:
        return x.decode('latin-1')

def cmd_run(list_cmd,
            return_output=False,
            env=None,
            working_dir=None,
            stdin=sp.PIPE,
            shell=False,
            dry_run=False,
            silent=False,
            process_input=None,
            stdout=None,
            stderr=None,
            no_commute=False,
            timeout=None,
            ):
    if not silent:
        x = ' '.join(map(str, list_cmd)) if isinstance(list_cmd, list) else list_cmd
        logger.info('start to cmd run: {}'.format(x))
        if working_dir:
            logger.info(working_dir)
    # if we dont' set stdin as sp.PIPE, it will complain the stdin is not a tty
    # device. Maybe, the reson is it is inside another process.
    # if stdout=sp.PIPE, it will not print the result in the screen
    e = os.environ.copy()
    if 'SSH_AUTH_SOCK' in e:
        del e['SSH_AUTH_SOCK']
    if working_dir:
        ensure_directory(working_dir)
    if env:
        for k in env:
            e[k] = env[k]
    if dry_run:
        # we need the log result. Thus, we do not return at teh very beginning
        return
    if not return_output:
        #if env is None:
            #p = sp.Popen(list_cmd, stdin=sp.PIPE, cwd=working_dir)
        #else:
        p = sp.Popen(' '.join(list_cmd) if shell else list_cmd,
                     stdin=stdin,
                     env=e,
                     shell=shell,
                     stdout=stdout,
                     cwd=working_dir,
                     stderr=stderr,
                     )
        if not no_commute:
            message = p.communicate(input=process_input, timeout=timeout)
            if p.returncode != 0:
                message = 'message = {}; cmd = {}'.format(
                    message, ' '.join(list_cmd))
                if stderr == sp.PIPE:
                    message += '; stderr = {}'.format(p.stderr.read().decode())
                raise ValueError(message)
            return message
        else:
            return p
    else:
        if isinstance(list_cmd, list) and shell:
            list_cmd = ' '.join(list_cmd)
        message = sp.check_output(list_cmd,
                                  env=e,
                                  cwd=working_dir,
                                  shell=shell,
                                  timeout=timeout,
                                  )
        if not silent:
            logger.info('finished the cmd run')
        return decode_to_str(message)

def ensure_directory(path):
    if path == '' or path == '.':
        return
    if path != None and len(path) > 0:
        assert not op.isfile(path), '{} is a file'.format(path)
        if not os.path.exists(path) and not op.islink(path):
            try:
                os.makedirs(path)
            except:
                if os.path.isdir(path):
                    # another process has done makedir
                    pass
                else:
                    raise

def parse_iteration(file_name):
    patterns = [
        '.*model(?:_iter)?_([0-9]*)\..*',
        '.*model(?:_iter)?_([0-9]*)e\..*',
        '.*model(?:_iter)?_([0-9]*)$',
    ]
    for p in patterns:
        r = re.match(p, file_name)
        if r is not None:
            return int(float(r.groups()[0]))
    logger.info('unable to parse the iterations for {}'.format(file_name))
    return -2

def load_list_file(fname):
    with open(fname, 'r') as fp:
        lines = fp.readlines()
    result = [line.strip() for line in lines]
    if len(result) > 0 and result[-1] == '':
        result = result[:-1]
    return result

def yaml_save(data, fname):
    tmp = fname + '.tmp'
    with open(tmp, 'w') as fp:
        yaml.dump(data, fp, default_flow_style=False, encoding='utf-8', allow_unicode=True)
    os.rename(tmp, fname)

def yaml_load(fname):
    if isinstance(fname, bytes):
        return yaml.unsafe_load(fname)
    with open(fname, 'r') as fp:
        return yaml.unsafe_load(fp)

class FileQueue(object):
    def __init__(self, folder):
        self.folder = folder
        ensure_directory(folder)

    def get_status_file(self):
        status_yaml = op.join(self.folder, 'status.yaml')
        return status_yaml

    def clear(self):
        # not sure if we need to add lock
        yaml_save({'start': 0, 'end': 0}, self.get_status_file())

    def put(self, ls):
        with acquire_lock(op.join(self.folder, 'lock')):
            status_yaml = self.get_status_file()
            if not op.isfile(status_yaml):
                status = {'start': 0, 'end': 0}
            else:
                status = yaml_load(status_yaml)
            idx = status['end']
            out_fname = op.join(self.folder, f'{idx:04d}')
            if isinstance(ls, str):
                ls = [ls]
            write_to_file('\n'.join(ls), out_fname)
            status['end'] += 1
            yaml_save(status, status_yaml)

    def pop(self, topk=1):
        while True:
            with acquire_lock(op.join(self.folder, 'lock')):
                if op.isfile(self.get_status_file()):
                    status = yaml_load(self.get_status_file())
                    if status['start'] != status['end']:
                        topk = min(status['end'] - status['start'], topk)
                        ret = []
                        for i in range(topk):
                            idx = status['start'] + i
                            out_fname = op.join(self.folder, f'{idx:04d}')
                            ret += load_list_file(out_fname)
                        status['start'] += topk
                        yaml_save(status, self.get_status_file())
                        return ret
            time.sleep(10)

