import os
import os.path as op
from pprint import pformat
import logging
from .azfuse import File
from .common import load_from_yaml_file
from .common import try_once
from .cloud_storage import create_cloud_fuse


def execute(task_type, **kwargs):
    if kwargs.get('name'):
        yaml_file = op.join('aux_data', 'azfuse', kwargs['name'] + '.yaml')
        os.environ['QD_CLOUD_FUSE_CONFIG_FILE'] = yaml_file
        os.environ['AZFUSE_CLOUD_FUSE_CONFIG_FILE'] = yaml_file
    if task_type in ['download', 'd']:
        File.prepare(kwargs['remainders'])
    elif task_type in ['cp']:
        raise NotImplementedError(task_type)
    elif task_type in ['ls']:
        assert len(kwargs['remainders']) == 1
        ret = File.list(kwargs['remainders'][0], return_info=True)
        from .common import print_table
        for r in ret:
            r['name'] = r['name'].replace(kwargs['remainders'][0], '')
        print_table(ret)
    elif task_type in ['url']:
        assert len(kwargs['remainders']) == 1
        c = create_cloud_fuse()
        logging.info(c.get_url(kwargs['remainders'][0]))
    elif task_type in ['head', 'tail', 'nvim', 'cat', 'display', 'open']:
        File.clear_cache(kwargs['remainders'])
        File.prepare(kwargs['remainders'])
        params = [File.get_cache_file(r) for r in kwargs['remainders']]
        from .common import cmd_run
        params.insert(0, task_type)
        cmd_run(
            params
        )
    elif task_type in ['update']:
        fname = kwargs['remainders'][0]
        params = [File.get_cache_file(r) for r in kwargs['remainders']]
        from .common import cmd_run
        params.insert(0, 'nvim')
        if File.isfile(fname):
            File.prepare([fname])
            pre = os.path.getmtime(params[-1])
        else:
            pre = 0
        from .common import ensure_directory
        ensure_directory(op.dirname(params[-1]))
        cmd_run(
            params
        )
        if os.path.getmtime(params[-1]) != pre:
            File.upload(params[-1], fname)
    elif task_type in ['rm']:
        for r in kwargs['remainders']:
            try_once(File.rm)(r)
    elif task_type in ['rmtree']:
        c = create_cloud_fuse()
        all_file = []
        for r in kwargs['remainders']:
            files = c.list(r, recursive=True, return_info=True)
            all_file.extend(files)
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(try_once(c.rm), f['name']) for f in all_file]
            from tqdm import tqdm
            [future.result() for future in tqdm(as_completed(futures))]
    elif task_type == 'cold':
        for r in kwargs['remainders']:
            File.set_access_tier(r, 'cold')
    elif task_type in ['u']:
        if kwargs.get('from'):
            c = create_cloud_fuse()
            yaml_file = op.join('aux_data', 'azfuse', kwargs['from'] + '.yaml')
            from_fuse = create_cloud_fuse(config=load_from_yaml_file(yaml_file))
            for data in kwargs['remainders']:
                c.upload_from_remote(data, from_fuse=from_fuse)
        else:
            c = create_cloud_fuse()
            for data in kwargs['remainders']:
                c.upload_from_cache(data)
    elif task_type in ['meta']:
        c = create_cloud_fuse()
        logging.info(c.get_meta(kwargs['remainders'][0]))
    elif task_type in ['clear_meta']:
        c = create_cloud_fuse()
        logging.info(c.set_meta({}, kwargs['remainders'][0]))
    elif task_type in ['break']:
        c = create_cloud_fuse()
        c.break_lease(kwargs['remainders'][0])
    else:
        assert 'Unknown {}'.format(task_type)

def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description='Azfuse')
    parser.add_argument('-c',
                        dest='name')
    parser.add_argument('-f',
                        dest='from')
    parser.add_argument('task_type',
                        choices=['d', 'download',
                                 'cp',
                                 'url',
                                 'ls',
                                 'cold',
                                 'rm',
                                 'rmtree',
                                 'head',
                                 'tail',
                                 'cat',
                                 'nvim',
                                 'display',
                                 'open',
                                 'update',
                                 'u', 
                                 'upload',
                                 'meta',
                                 'clear_meta',
                                 'break',
                                 ])
    parser.add_argument('remainders', nargs=argparse.REMAINDER,
            type=str)
    return parser.parse_args()

if __name__ == '__main__':
    from .common import init_logging
    init_logging()
    args = parse_args()
    param = vars(args)
    execute(**param)

