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
import subprocess
import json
import shlex



def task_rsync_cards():
    def rsync_cards(dependencies, targets):
        cameras = pd.read_csv(dependencies[0],index_col='CameraSerialNumber',parse_dates=['CreateDate'])
        cameras = cameras.groupby('mountpoint').first()
        hosts = cameras.host.unique()
        cameras['RsyncDone']= False
        processes = set()
        max_processes = 4
        workon = True
        hosts = cameras.host.unique()
        for host in hosts:
            item =cameras[cameras['host']==host].iloc[0]
            os.makedirs(item.Destination,  exist_ok=True)
            command = shlex.split(f"rsync -a -W  --exclude '*.LRV' --exclude '*.THM' --progress  {item.name} {item.Destination}")
            processes.add(subprocess.Popen(command))#,startupinfo=
            if len(processes) >= max_processes:
                os.wait()
            processes.difference_update([p for p in processes if p.poll() is not None])
        for host in hosts:
            if len(cameras[cameras['host']==host])==2: #check to see if there were two cards on the host
                item =cameras[cameras['host']==host].iloc[-1]
                os.makedirs(item.Destination, exist_ok=True)
                command = shlex.split(f"rsync -a -W  --exclude '*.LRV' --exclude '*.THM' --progress  {item.name} {item.Destination}")
                processes.add(subprocess.Popen(command))
                if len(processes) >= max_processes:
                    os.wait()
                processes.difference_update([p for p in processes if p.poll() is not None])
        for p in processes:
            if p.poll() is None:
                p.wait()

    backupsessions = pd.read_csv(config.geturl('backupsessions'),index_col='TimeStamp',parse_dates=['TimeStamp']).sort_values('TimeStamp')
    file_dep = f"{config.geturl('cardstore')}/backup_{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}.csv"
    target = f"{config.geturl('cardstore')}/backup_{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}_log.csv"
    return {
        'file_dep':[file_dep],
        'actions':[rsync_cards],
        'targets':[target],
        'uptodate':[False],
        'clean':True,
    }


# def task_rsync_cards_remove():
#     def rsync_cards(dependencies, targets):
#         cameras = pd.read_csv(dependencies[0],index_col='CameraSerialNumber',parse_dates=['CreateDate'])
#         cameras = cameras.groupby('mountpoint').first().reset_index().set_index('mountpoint')
#         cameras['RsyncDone']= False
#         processes = set()
#         max_processes = 4
#         for index,row in cameras.iterrows():
#             os.makedirs(row.Destination, exist_ok=True)
#             command = shlex.split(f"rsync -a -W  --exclude '*.LRV' --exclude '*.THM' --progress --remove-source-files  {index} {row.Destination}")
#             processes.add(subprocess.Popen(command))
#             if len(processes) >= max_processes:
#                 os.wait()
#             processes.difference_update([p for p in processes if p.poll() is not None])
#         for p in processes:                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
#             if p.poll() is None:
#                 p.wait()

#     backupsessions = pd.read_csv(config.geturl('backupsessions'),index_col='TimeStamp',parse_dates=['TimeStamp']).sort_values('TimeStamp')
#     file_dep = f"{config.geturl('cardstore')}/backup_{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}.csv"
#     target = f"{config.geturl('cardstore')}/backup_{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}_log_remove.csv"
#     return {
#         'file_dep':[file_dep],
#         'actions':[rsync_cards],
#         'targets':[target],
#         'uptodate':[False],
#         'clean':True,
#     }



# def task_rsync_remove_LVR():
#     def rsync_cards(dependencies, targets):
#         cameras = pd.read_csv(dependencies[0],index_col='CameraSerialNumber',parse_dates=['CreateDate'])
#         cameras['SourceDir'] = cameras['SourceFile'].apply(os.path.dirname)
#         cameras = cameras.groupby('SourceDir').first()
#         cameras['RsyncDone']= False
#         processes = set()
#         max_processes = 4
#         for index,row in cameras.iterrows():
#             os.makedirs(row.Destination, exist_ok=True)
#             command = shlex.split(f"find {os.path.dirname(row.SourceFile)}  -type f -name '*.LRV' -delete" )
#             processes.add(subprocess.Popen(command))
#             if len(processes) >= max_processes:
#                 os.wait()
#             processes.difference_update([p for p in processes if p.poll() is not None])
#         for p in processes:
#             if p.poll() is None:
#                 p.wait()

#     backupsessions = pd.read_csv(config.geturl('backupsessions'),index_col='TimeStamp',parse_dates=['TimeStamp']).sort_values('TimeStamp')
#     file_dep = f"{config.geturl('cardstore')}/backup_{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}.csv"
#     target = f"{config.geturl('cardstore')}/backup_{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}_log_remove_lvr.csv"
#     return {
#         'file_dep':[file_dep],
#         'actions':[rsync_cards],
#         'targets':[target],
#         'uptodate':[False],
#         'clean':True,
#     }
# def task_rsync_remove_THM():
#     def rsync_cards(dependencies, targets):
#         cameras = pd.read_csv(dependencies[0],index_col='CameraSerialNumber',parse_dates=['CreateDate'])
#         cameras['SourceDir'] = cameras['SourceFile'].apply(os.path.dirname)
#         cameras = cameras.groupby('SourceDir').first()
#         cameras['RsyncDone']= False
#         processes = set()
#         max_processes = 4
#         for index,row in cameras.iterrows():
#             os.makedirs(row.Destination, exist_ok=True)
#             command = shlex.split(f"find {os.path.dirname(row.SourceFile)}  -type f -name '*.THM' -delete" )
#             processes.add(subprocess.Popen(command))
#             if len(processes) >= max_processes:
#                 os.wait()
#             processes.difference_update([p for p in processes if p.poll() is not None])
#         for p in processes:
#             if p.poll() is None:
#                 p.wait()

#     backupsessions = pd.read_csv(config.geturl('backupsessions'),index_col='TimeStamp',parse_dates=['TimeStamp']).sort_values('TimeStamp')
#     file_dep = f"{config.geturl('cardstore')}/backup_{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}.csv"
#     target = f"{config.geturl('cardstore')}/backup_{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}_log_remove_thm.csv"
#     return {
#         'file_dep':[file_dep],
#         'actions':[rsync_cards],
#         'targets':[target],
#         'uptodate':[False],
#         'clean':True,

#     }

# def task_card_report():
#     def rsync_cards(dependencies, targets):
#         cameras = pd.read_csv(dependencies[0],index_col='CameraSerialNumber',parse_dates=['CreateDate'])
#         cameras['RsyncDone']= False
#         processes = set()
#         max_processes = 4
#         for index,row in cameras.iterrows():
#             os.makedirs(row.Destination, exist_ok=True)
#             command = shlex.split(f"find {os.path.dirname(row.SourceFile)}  -type f -name '*.THM' -delete" )
#             processes.add(subprocess.Popen(command))
#             if len(processes) >= max_processes:
#                 os.wait()
#             processes.difference_update([p for p in processes if p.poll() is not None])
#         for p in processes:
#             if p.poll() is None:
#                 p.wait()

#     backupsessions = pd.read_csv(config.geturl('backupsessions'),index_col='TimeStamp',parse_dates=['TimeStamp']).sort_values('TimeStamp')
#     file_dep = f"{config.geturl('cardstore')}/backup_{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}.csv"
#     target = f"{config.geturl('cardstore')}/backup_{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}_log_remove_thm.csv"
#     return {
#         'file_dep':[file_dep],
#         'actions':[rsync_cards],
#         'targets':[target],
#         'uptodate':[False],
#         'clean':True,
#     }

if __name__ == '__main__':
    import doit
    DOIT_CONFIG = {'check_file_uptodate': 'timestamp'}
    #print(globals())
    doit.run(globals())