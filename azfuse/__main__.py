import os
import os.path as op
from pprint import pformat
import logging
from .azfuse import File


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
        print_table(ret)
    elif task_type in ['head']:
        File.prepare(kwargs['remainders'])
        params = [File.get_cache_file(r) for r in kwargs['remainders']]
        from .common import cmd_run
        params.insert(0, 'head')
        cmd_run(
            params
        )
    else:
        assert 'Unknown {}'.format(task_type)

def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description='Azfuse')
    parser.add_argument('-c',
                        dest='name')
    parser.add_argument('task_type',
                        choices=['d', 'download',
                                 'cp',
                                 'ls',
                                 'head',
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

