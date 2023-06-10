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
from gpmfio import extract_gpmf_stream
from gpmfstream import parse_block
from gpmfstream import extract_blocks
import numpy as np


#exiftool -api largefilesupport=1  -ee -G3 -X -b -json 
# def task_stream_bin():
#         def parsefile(dependencies, targets):
#              estream = extract_gpmf_stream(dependencies[0])
#              with open(targets[0], 'wb') as file:
#                 file.write(estream)

#         file_dep = glob.glob(f"{config.geturl('videosource')}**/100GOPRO/GX01*.MP4",recursive=True)
#         for file in file_dep:
#             target = file.replace('MP4','bin')
#             if file!=target:
#                 yield { 
#                     'name':target,
#                     'file_dep':[file],
#                     'actions':[parsefile],
#                     'targets':[target],
#                     'uptodate':[True],
#                     'clean':True,
#                 } 

def task_create_json():

        def parsefile(dependencies, targets):
            with open(dependencies[0], 'rb') as file:
                stream =file.read()
                keys=['GPS5','ACCL','MWET','CORI']
                gps_blocks = extract_blocks(stream,keys)  
                blocks = {
                    s: [] for s in keys
                }
                gps_data = list(map(parse_block, gps_blocks))
                for item in gps_data:
                     key =list(item.keys())[0]
                     blocks[key].append(item[key]._asdict())
                gps = pd.DataFrame(blocks['GPS5']).explode(['latitude','longitude','altitude','speed_2d','speed_3d'])
                gps.loc[gps.assign(d=gps.stmp).duplicated('stmp'), ['timestamp','stmp']] = np.nan
                gps.timestamp=pd.to_numeric(pd.to_datetime(gps.timestamp))
                gps.loc[gps.timestamp<0,'timestamp']=np.nan
                gps = gps.interpolate()
                gps.timestamp=pd.to_datetime(gps.timestamp)
                cori=pd.DataFrame(blocks['CORI']).explode('data')
                cori.loc[gps.assign(d=cori.stmp).duplicated('stmp'), 'stmp'] = np.nan
                cori=pd.DataFrame(blocks['CORI']).explode('data')
                gps_dict = [block._asdict() for block in gps_data]
                acc_blocks = extract_blocks(stream,'ACCL')  
                #acc_data = list(map(parse_acc_block, acc_blocks)) 
                # Create DataFrame from the list of dictionaries
                df = pd.DataFrame(gps_dict)
                df.latitude =df.latitude.apply(np.mean)
                df.longitude =df.longitude.apply(np.mean)
                df.to_csv(targets[0])
                pass


        file_dep = glob.glob(f"{config.geturl('videosource')}/**/100GOPRO/GX01*.bin",recursive=True)
        for file in file_dep:
            target = file.replace('bin','nc')
            yield { 
                'name':target,
                'file_dep':[file],
                'actions':[parsefile],
                'targets':[target],
                'uptodate':[True],
                'clean':True,
            }
# def task_scan_raw():
#     def get_serialnumbers(dependencies, targets):
#         backupsessions = pd.read_csv(config.geturl('backupsessions'),index_col='TimeStamp',parse_dates=['TimeStamp']).sort_values('TimeStamp')
#         barnumbers = pd.read_csv(config.geturl('barnumbers')) 
#         mountpoints = pd.DataFrame(json.loads(subprocess.getoutput('lsblk -J -o  NAME,SIZE,FSTYPE,TYPE,MOUNTPOINT'))['blockdevices'])
#         mountpoints = mountpoints[~mountpoints.children.isna()]
#         mountpoints =pd.DataFrame(mountpoints.children.apply(lambda x: x[0]).to_list())[['name','mountpoint']]
#         paths = pd.DataFrame(subprocess.getoutput('udevadm info -q path -n $(ls /dev/s*1)').splitlines(),columns=['Path'])
#         paths[['host','dev']]=paths.Path.str.extract(r'(?P<host>host\d+).*block\/(?P<dev>([^\\]+$))')[['host','dev']]
#         paths['name'] =paths.dev.str.split('/',expand=True)[1]
#         mountpoints =mountpoints.merge(paths, on='name', how='inner')
#         cameras = []
#         for item in dependencies:
#             filter = os.path.join(item,config.cfg['paths']['videowild'])
#             files = glob.glob(filter)
#             if files:
#                 command = f"exiftool -api largefilesupport=1 -u  -json -ext MP4 -q -CameraSerialNumber -CreateDate -SourceFile -Duration -FileSize -FieldOfView {item}"
#                 process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#                 out, err = process.communicate()
#                 s=str(out,'utf-8')
#                 data = StringIO(s)  
#                 df =pd.read_json(data)
#                 cameras.append(df)
#         cameras = pd.concat(cameras)
#         cameras['CreateDate'] = pd.to_datetime(cameras['CreateDate'],format='%Y:%m:%d %H:%M:%S')
#         cameras =cameras.merge(barnumbers, on='CameraSerialNumber', how='inner')
#         cameras['mountpoint'] = cameras.SourceFile.str.extract(r'(?P<mount>.*)\/DCIM')
#         cameras =cameras.merge(mountpoints, on='mountpoint', how='inner')
#         hosts =cameras.host.unique()
#         cameras['Destination'] =''
#         for i in range(0,len(hosts)):
#             #path = f'{destinations[i%4]}/raw/{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}'
#             path = f"{config.geturl('cardstore')}/{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}"
#             os.makedirs(path,exist_ok=True)
#             cameras.loc[cameras.host==hosts[i],'path'] = path
#         cameras['Destination']=cameras.apply(lambda x: f"{x.path}/{x.CameraNumber}_{x.CameraSerialNumber}_{x.CreateDate:%Y%m%dT%H%M}",axis=1)
#         #path = f"{config.geturl('cardstore')}/{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}"
#         cameras.to_csv(targets[0],index=False)
#         if cameras.mountpoint.apply(os.path.basename).apply(len).max()>9:
#             print('WARNING duplicate card numbers!')


#     gopro = glob.glob('/media/*/*/DCIM/100GOPRO')
#     backupsessions = pd.read_csv(config.geturl('backupsessions'),index_col='TimeStamp',parse_dates=['TimeStamp']).sort_values('TimeStamp')
#     target = f"{config.geturl('cardstore')}/backup_{backupsessions.iloc[-1].name:%Y%m%dT%H%M%S}.csv"
#     return {
#         'file_dep':gopro,
#         'actions':[get_serialnumbers],
#         'targets':[target],
#         'uptodate':[False],
#         'clean':True,
#     }

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
    DOIT_CONFIG = {'check_file_uptodate': 'timestamp','continue': True} #,'num_threads': 4}
    #print(globals())
    doit.run(globals())