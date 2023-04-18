from ast import Pass
import doit
from doit import get_var
from doit.tools import run_once
from doit import create_after
import glob
from numpy import int32
import yaml
import os
import pandas as pd
import shutil
import config



def task_setup():
    config.read_config()



def task_createbackupset():
    def create_backupsessions(dependencies, targets):
        timestamp = pd.Timestamp.now()
        data ={'TimeStamp':[timestamp],
        'Destination':[f"{config.geturl('output')}/{timestamp:%Y%m%dT%H%M%S}"]}
        df = pd.DataFrame.from_dict(data)
        df = df.set_index('TimeStamp')
        if os.path.exists(targets[0]):
            df1 = pd.read_csv(targets[0],index_col='TimeStamp',parse_dates=['TimeStamp'])
            df = pd.concat([df1,df])
        df.to_csv(targets[0])
    backupsessions = config.geturl('backupsessions')
    return {
        'actions':[create_backupsessions],
        'targets':[config.geturl('backupsessions')],
        'uptodate':[False],
        'clean':True,
    }
         
if __name__ == '__main__':
    import doit
    DOIT_CONFIG = {'check_file_uptodate': 'timestamp'}
    #print(globals())
    doit.run(globals())
    