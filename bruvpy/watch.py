import doit


import glob

def task_xxx():
    """my doc"""
    LIST = glob.glob('/home/card/test/') # might be empty
    yield {
        'basename': 'do_x',
        'name': None,
        'doc': 'docs for X',
        'watch': ['/home/card/test/'],
        'actions': ['echo loooking'],
        }
    for item in LIST:
        yield {
            'basename': 'do_x',
            'name': item,
            'actions': ['echo %s' % item],
            'verbosity': 2,
            }

if __name__ == '__main__':
    import doit
    DOIT_CONFIG = {'check_file_uptodate': 'timestamp'}
    #print(globals())
    doit.run(globals())