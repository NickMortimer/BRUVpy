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
import ast
from io import StringIO

destinations = ['/media/mor582/FBackup1/DR2023-02/fieddata/BRUVS/',
                '/media/mor582/FBackup2/DR2023-02/fielddata/BRUVS/',
                '/media/mor582/FieldTrip2/DR2023-02/fielddata/BRUVS/',
                '/media/mor582/FieldTrip4/DR2023-02/fielddata/BRUVS/',
                ]

def rsync(sources,dest,move=False):
    output = []
    for source in sources.flatten():
        os.makedirs(dest,exist_ok=True)
        print(f'{source} {os.path.exists(source)}')
        if os.path.exists(source):
            if move:
                command =f"rsync -a -W --remove-source-files --exclude '*.LRV' --exclude '*.THM' --progress  {source} {dest}"
            else:
                command =f"rsync   -a -W --exclude '*.LRV' --exclude '*.THM' --progress  {source} {dest}"
            output.append(subprocess.getoutput(command))
    return output

def task_scan_serialnumbers():
    def get_serialnumbers(dependencies, targets):
        backupsessions = pd.read_csv(config.geturl('backupsessions'),index_col='TimeStamp',parse_dates=['TimeStamp']).sort_values('TimeStamp')
        barnumbers = pd.read_csv(config.geturl('barnumbers')) 
        mountpoints = pd.DataFrame(json.loads(subprocess.getoutput('lsblk -J -o  NAME,SIZE,FSTYPE,TYPE,MOUNTPOINT'))['blockdevices'])
        mountpoints = mountpoints[~mountpoints.children.isna()]
        mountpoints =pd.DataFrame(mountpoints.children.apply(lambda x: x[0]).to_list())[['name','mountpoint']]
        paths = pd.DataFrame(subprocess.getoutput('udevadm info -q path -n $(ls /dev/s*1)').splitlines(),columns=['Path'])
        paths[['host','dev']]=paths.Path.str.extract(r'(?P<host>host\d+).*block\/(?P<dev>([^\\]+$))')[['host','dev']]
        paths['name'] =paths.dev.str.split('/',expand=True)[1]
        mountpoints =mountpoints.merge(paths, on='name', how='inner')
        cameras = []
        for item in dependencies:
            filter = os.path.join(item,config.cfg['paths']['videowild'])
            files = glob.glob(filter)
            if files:
                command = f"exiftool -api largefilesupport=1 -u  -json -ext MP4 -q -CameraSerialNumber -CreateDate -SourceFile -Duration -FileSize -FieldOfView {item}"
                process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, err = process.communicate()
                s=str(out,'utf-8')
                data = StringIO(s)  
                df =pd.read_json(data)
                cameras.append(df)
        cameras = pd.concat(cameras)
        cameras['CreateDate'] = pd.to_datetime(cameras['CreateDate'],format='%Y:%m:%d %H:%M:%S')
        cameras =cameras.merge(barnumbers, on='CameraSerialNumber', how='inner')
        cameras['mountpoint'] = cameras.SourceFile.str.extract(r'(?P<mount>.*)\/DCIM')
        cameras =cameras.merge(mountpoints, on='mountpoint', how='inner')
        hosts =cameras.host.unique()
        cameras['Destination'] =''
        for i in range(0,len(hosts)):
            #path = f'{destinations[i%4]}/raw/{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}'
            path = f"{config.geturl('cardstore')}/{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}"
            os.makedirs(path,exist_ok=True)
            cameras.loc[cameras.host==hosts[i],'path'] = path
        cameras['Destination']=cameras.apply(lambda x: f"{x.path}/{x.CameraNumber}_{x.CameraSerialNumber}_{x.CreateDate:%Y%m%dT%H%M}",axis=1)
        #path = f"{config.geturl('cardstore')}/{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}"
        cameras.to_csv(targets[0],index=False)
        if cameras.mountpoint.apply(os.path.basename).apply(len).max()>9:
            print('WARNING duplicate card numbers!')


    gopro = glob.glob('/media/*/*/DCIM/100GOPRO')
    backupsessions = pd.read_csv(config.geturl('backupsessions'),index_col='TimeStamp',parse_dates=['TimeStamp']).sort_values('TimeStamp')
    target = f"{config.geturl('cardstore')}/backup_{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}.csv"
    return {
        'file_dep':gopro,
        'actions':[get_serialnumbers],
        'targets':[target],
        'uptodate':[False],
        'clean':True,
    }

# def task_scan_devs():
#     result = subprocess.getoutput('lsblk -J -o  NAME,SIZE,FSTYPE,TYPE,MOUNTPOINT')
    

# def task_process_cameras():
#     def process_cameras(dependencies, targets):
#         backupsessions = pd.read_csv(config.geturl('backupsessions'),index_col='TimeStamp',parse_dates=['TimeStamp']).sort_values('TimeStamp')
#         barnumbers = pd.read_csv(config.geturl('barnumbers'))
#         cameras = []
#         for item in dependencies:
#             filter = os.path.join(item,config.cfg['paths']['videowild'])
#             files = glob.glob(filter)
#             if files:
#                 command = f"exiftool -api largefilesupport=1 -u  -json -ext MP4 -q -CameraSerialNumber -CreateDate -SourceFile -Duration -FileSize -FieldOfView {item}"
#                 result =subprocess.getoutput(command)
#                 try:
#                     df =pd.read_json(result)
#                     cameras.append(df)
#                 except:
#                     print('No json')
#         cameras = pd.concat(cameras)
#         cameras['CreateDate'] = pd.to_datetime(cameras['CreateDate'],format='%Y:%m:%d %H:%M:%S')
#         cameras =cameras.merge(barnumbers, on='CameraSerialNumber', how='inner')
#         path = f"{config.geturl('cardstore')}/{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}"
#         os.makedirs(path,exist_ok=True)
#         cameras['Destination']=cameras.apply(lambda x: f"{path}/{x.CameraNumber}_{x.CameraSerialNumber}_{x.CreateDate:%Y%m%dT%H%M}",axis=1)
#         cameras.to_csv(targets[0],index=False)



#     gopro = glob.glob('/media/*/*/DCIM/100GOPRO')
#     backupsessions = pd.read_csv(config.geturl('backupsessions'),index_col='TimeStamp',parse_dates=['TimeStamp']).sort_values('TimeStamp')
#     target = f"{config.geturl('cardstore')}/backup_{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}.csv"
#     return {
#         'file_dep':gopro,
#         'actions':[get_serialnumbers],
#         'targets':[target],
#         'uptodate':[True],
#         'clean':True,
#     }


         
if __name__ == '__main__':
    import doit
    DOIT_CONFIG = {'check_file_uptodate': 'timestamp'}
    #print(globals())
    doit.run(globals())