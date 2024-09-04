from azfuse import File


def test_open_to_write():
    from azfuse.common import init_logging
    init_logging()
    with File.open('data/abc.txt', 'w') as fp:
        fp.write('abc')

def test_open_to_read():
    from azfuse.common import init_logging
    init_logging()
    with File.open('data/abc.txt', 'w') as fp:
        fp.write('abc')
    File.clear_cache('data/abc.txt')

    with File.open('data/abc.txt', 'r') as fp:
        x = fp.read()

    assert x == 'abc'

def test_prepare_in_batch():
    from azfuse.common import init_logging
    init_logging()
    from azfuse import File
    num = 5
    for i in range(num):
        with File.open('data/batch_test/{}.txt'.format(i), 'w') as fp:
            fp.write(str(i))
    File.clear_cache('data/batch_test')

    File.prepare(['data/batch_test/{}.txt'.format(i) for i in range(num)])
    for i in range(num):
        with File.open('data/batch_test/{}.txt'.format(i), 'r') as fp:
            content = fp.read()
            assert content == str(i)

def test_async_upload():
    from azfuse.common import init_logging
    init_logging()
    from azfuse import File
    num = 5
    with File.async_upload(enabled=True):
        for i in range(num):
            with File.open('data/batch_test/{}.txt'.format(i), 'w') as fp:
                fp.write(str(i + 10))
    File.clear_cache('data/batch_test')

    File.prepare(['data/batch_test/{}.txt'.format(i) for i in range(num)])
    for i in range(num):
        with File.open('data/batch_test/{}.txt'.format(i), 'r') as fp:
            content = fp.read()
            assert content == str(i + 10)

