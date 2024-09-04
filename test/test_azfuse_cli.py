from azfuse import File


def test_azfuse_download_one_file():
    from azfuse.common import cmd_run
    fname = 'data/pytest/azfuse/single_file.txt'
    if File.isfile(fname):
        File.rm(fname)
    with File.open(fname, 'w') as fp:
        fp.write('abc')
    File.clear_cache(fname)

    cmd = ['python', '-m', 'azfuse', 'd', fname]
    cmd_run(cmd)

    with open(fname, 'r') as fp:
        assert fp.read() == 'abc'

def test_azfuse_download_multi_file():
    from azfuse.common import cmd_run
    fnames = ['data/pytest/azfuse/f1.txt', 
              'data/pytest/azfuse/f2.txt']
    for fname in fnames:
        if File.isfile(fname):
            File.rm(fname)
        with File.open(fname, 'w') as fp:
            fp.write('abc')
        File.clear_cache(fname)

    cmd = ['python', '-m', 'azfuse', 'd'] + fnames
    cmd_run(cmd)

    for fname in fnames:
        with open(fname, 'r') as fp:
            assert fp.read() == 'abc'

