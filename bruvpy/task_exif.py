import doit
from doit import get_var
from doit.tools import run_once
from doit import create_after
import glob
import yaml
import os
import pandas as pd
import shutil

import config
from config import task_read_config




def task_init():
    
    os.makedirs(os.path.join(config.basepath,config.cfg['paths']['process']),exist_ok=True)

def task_create_json():
        exifpath = os.path.join(config.basepath,config.cfg['paths']['exiftool'])
        for item in glob.glob(os.path.join(config.basepath,config.cfg['paths']['videosource']),recursive=True):
            if glob.glob(os.path.join(item,config.cfg['paths']['videowild'].upper())) or os.path.join(item,config.cfg['paths']['videowild'].lower()):
                target  = os.path.join(item,config.cfg['paths']['exifname'])
                filter = os.path.join(item,config.cfg['paths']['videowild'])
                file_dep = glob.glob(filter)
                if file_dep:
                    yield { 
                        'name':item,
                        'actions':[f'exiftool -api largefilesupport=1 -u  -json {filter} > {target}'],
                        'targets':[target],
                        'uptodate':[True],
                        'clean':True,
                    }        

@create_after(executed='create_json', target_regex='.*\.json')                   
def task_concat_json():
    def concatfiles(dependencies, targets):
        def open_exif(file):
            df = pd.read_json(file)
            df = df[df.columns[df.columns.isin(config.cfg['processing']['filterexif'])]]
            return df
            
        exbasepath = os.path.dirname(targets[0])
        exif = pd.concat([open_exif(file) for file in dependencies])
        exif['RelPath'] = exif.apply(lambda x: os.path.relpath(x['SourceFile'],exbasepath),axis=1)
        exif.to_csv(targets[0],index=False,escapechar='"')
        pass
    file_dep = glob.glob( os.path.join(config.basepath,config.cfg['paths']['videosource'],config.cfg['paths']['exifname']),recursive=True)
    target =  os.path.join(config.basepath,config.cfg['paths']['process'],config.cfg['paths']['exifcat'])
    return {
        'file_dep':file_dep,
        'actions':[concatfiles],
        'targets':[target],
        'uptodate':[True],
        'clean':True,
    }
 
# def task_extract_group_time():
#     def calcnames(dependencies, targets):
#         exif = pd.read_csv(dependencies[0])
#         exif.to_csv(targets[0],index=False)
#         exif[['ItemId','GroupId']]=exif.FileName.str.extract('(?P<item>\d\d)(?P<group>\d\d\d\d).MP4')
#         timing =exif.groupby(['CameraSerialNumber','GroupId','Directory']).first().reset_index()[['GroupId','FileName','Directory','CameraSerialNumber','MediaCreateDate']]
#         timing['CorrectedMediaCreateDate'] = timing['MediaCreateDate']
#         timing = timing.sort_values(['MediaCreateDate','Directory'])
#         timing.to_csv(targets[0],index=False)
#     file_dep = os.path.join(config.basepath,config.cfg['paths']['process'],config.cfg['paths']['exifcat'])
#     target =  os.path.join(config.basepath,config.cfg['paths']['process'],config.cfg['paths']['timename'])
#     return {
#         'file_dep':[file_dep],
#         'actions':[calcnames],
#         'targets':[target],
#         'uptodate':[True],
#         'clean':True,
#     }

# def task_set_video_time():
#     file_dep = os.path.join(config.basepath,config.cfg['paths']['process'],config.cfg['paths']['timename'])
#     if os.path.exists(file_dep):
#         times = pd.read_csv(file_dep)
#         times =times[times.MediaCreateDate!=times.CorrectedMediaCreateDate]
#         for index,row in times.iterrows():
#             pass
#             # yield { 
#             #     'name':item,
#             #     'actions':[f'exiftool -api largefilesupport=1 -u  -json {filter} > {target}'],
#             #     'targets':[target],
#             #     'uptodate':[True],
#             #     'clean':True,
#             # } 


 
# @create_after(executed='create_videolist', target_regex='.*\.csv')   
# def task_create_json():
#     global config.basepath,config.cfg
#     exifpath = os.path.abspath(os.path.join(config.basepath,config.cfg['paths']['exiftool']))
#     videolist =  os.path.join(config.basepath,config.cfg['paths']['videolist'])
#     vconfig.basepath = os.path.dirname(videolist)
#     if os.path.exists(videolist):
#         video = pd.read_csv(videolist)
#         paths =video.groupby('RelName')
#         for name,df in paths:
#             source = os.path.abspath(os.path.join(vconfig.basepath,name))
#             filter = os.path.join(source,config.cfg['paths']['videowild'])
#             target = os.path.join(source,config.cfg['paths']['exifname'])
#             yield {
#                 'name':name,
#                 'actions':[f'"{exifpath}" -api largefile=1 -u -json "{filter}" > "{target}"'],
#                 'targets':[target],
#                 'uptodate':[run_once],
#                 'clean':True,
#             }

    
# def task_match_cameras():
#     def matchcameras(dependencies, targets):
#         exifdf = pd.read_csv(os.path.join(config.basepath,config.cfg['paths']['exiflist']),index_col='CameraSerialNumber')
#         exifdf = exifdf[exifdf.columns[exifdf.columns.isin(config.cfg['exifcols'])]]
#         exifdf['EndTime'] = pd.to_datetime(exifdf.FileModifyDate,format='%Y:%m:%d  %H:%M:%S%z')
#         exifdf['StartTime'] =exifdf['EndTime'] - pd.to_timedelta(exifdf.Duration)
#         bruv = pd.read_csv(os.path.join(config.basepath,config.cfg['paths']['bruvconfig']),index_col='CameraSerialNumber')
#         bruv=exifdf.join(bruv)
#         bruv.to_csv(targets[0],index=True)
#         pass
#     file_dep = [os.path.join(config.basepath,config.cfg['paths']['exiflist']),os.path.join(config.basepath,config.cfg['paths']['bruvconfig'])]
#     target =  os.path.join(config.basepath,config.cfg['paths']['camlist'])
#     return {
#         'file_dep':file_dep,
#         'actions':[matchcameras],
#         'targets':[target],
#         'uptodate':[True],
#         'clean':True,
#     }    
    
# def task_report_cameras():
#     def camreport(dependencies, targets):
#         def processframe(df):
#             leftstart = df[df.Position=='LFT'].StartTime.min()
#             rightstart = df[df.Position=='RGT'].StartTime.min()
#             # Right is behind left
#             if (leftstart - rightstart).total_seconds()/60 >20:
#                 df.loc[df.Position=='RGT','StartTime'] = df.loc[df.Position=='RGT','StartTime'] +(leftstart - rightstart)
#                 df.loc[df.Position=='RGT','EndTime'] = df.loc[df.Position=='RGT','EndTime'] +(leftstart - rightstart)
#             elif (leftstart - rightstart).total_seconds()/60 <-20:
#                 df.loc[df.Position=='LFT','StartTime'] = df.loc[df.Position=='LFT','StartTime'] -(leftstart - rightstart)
#                 df.loc[df.Position=='LFT','EndTime'] = df.loc[df.Position=='LFT','EndTime'] -(leftstart - rightstart)
            
#             return df

#         camlist = pd.read_csv(dependencies[0],parse_dates=['StartTime','EndTime','Duration'])
#         camlist =camlist.groupby('BarNumber').apply(processframe)
#         camlist.to_csv(targets[0],index=False)
#         pass
#     file_dep = [os.path.join(config.basepath,config.cfg['paths']['camlist'])]
#     target =  [os.path.join(config.basepath,config.cfg['paths']['camreport'])]
#     return {
#         'file_dep':file_dep,
#         'actions':[camreport],
#         'targets':target,
#         'uptodate':[False],
#         'clean':True,
#     } 

# def task_calc_rename():
#     def calcrename(dependencies, targets):
#         def calc_rename(df,cams):
#             operation = cams[(cams.StartTime>df.StartTime-pd.to_timedelta('5min')) & (cams.EndTime<df.EndTime+pd.to_timedelta('5min')) & (df.BarNumber== cams.BarNumber) ].copy().sort_values('StartTime')
#             operation['Operation'] = df.Operation
#             operation['FieldTrip'] = df.FieldTrip
#             operation['FileNumber'] =1
#             operation.loc[operation.Position=='RGT','FileNumber']=operation.loc[operation.Position=='RGT','FileNumber'].cumsum()
#             operation.loc[operation.Position=='LFT','FileNumber']=operation.loc[operation.Position=='LFT','FileNumber'].cumsum()
#             operation['ProposedName'] = operation.apply(lambda x: f"LRUVS_{x['Position']}_{x['FieldTrip']}_{x['Operation']:03}_{x['StartTime'].strftime('%Y%m%dT%H%M%S')}_{x['FileNumber']:03}",axis=1)
#             return operation
#         # in calibration mode
#         cams = pd.read_csv(os.path.join(config.basepath,config.cfg['paths']['camreport']),parse_dates=['StartTime','EndTime']).sort_values('StartTime')
#         if 'calibration' in config.cfg:
#             operation = cams
#             operation['Operation'] = operation['BarNumber']
#             operation['FieldTrip'] = config.cfg['calibration']['name']
#             operation['FileNumber'] =1
#             operation['FileNumber']=operation.groupby(by=['BarNumber','Position'])['FileNumber'].cumsum()              
#             operation['ProposedName'] = operation.apply(lambda x: f"LRUVS_{x['Position']}_{x['FieldTrip']}_{x['Operation']:03}_{x['StartTime'].strftime('%Y%m%dT%H%M%S')}_{x['FileNumber']:03}",axis=1)
#             operation.to_csv(targets[0],index=False)
#         else:                
#             trips = pd.read_csv(os.path.join(config.basepath,config.cfg['paths']['tripsheet']),parse_dates=['StartTime','EndTime'])
#             matched = pd.concat(list(trips.apply(calc_rename,cams=cams,axis=1)))
#             matched.to_csv(targets[0],index=False)
#     if 'calibration' in config.cfg:
#         file_dep = [os.path.join(config.basepath,config.cfg['paths']['camreport'])]
#     else:
#         file_dep = [os.path.join(config.basepath,config.cfg['paths']['tripsheet']),os.path.join(config.basepath,config.cfg['paths']['camreport'])]
#     target =  [os.path.join(config.basepath,config.cfg['paths']['rename'])]
#     return {
#         'file_dep':file_dep,
#         'actions':[calcrename],
#         'targets':target,
#         'uptodate':[False],
#         'clean':True,
#     } 
    

# def task_mov_files():
#     def movefiles(dependencies, targets):
#         files = pd.read_csv(dependencies[0])
#         path = os.path.dirname(dependencies[0])
#         with open(targets[0], 'w') as f:
#             for index,row in files.iterrows():
#                 source = os.path.abspath(os.path.join(path,row['RelPath']))
#                 dest = os.path.abspath(os.path.join(config.cfg['paths']['destpath'],f"{row['FieldTrip']}_{row['Operation']:03}",row['ProposedName']+os.path.splitext(row['FileName'])[1]))
#                 os.makedirs(os.path.dirname(dest),exist_ok=True)
#                 f.write(f'move {source} -> {dest}')
#                 if os.path.exists(source):
#                     shutil.move(source,dest)
            
            
 
#     file_dep = [os.path.join(config.basepath,config.cfg['paths']['rename'])]
#     target =  [os.path.join(config.basepath,config.cfg['paths']['movelog'])]
#     return {
#         'file_dep':file_dep,
#         'actions':[movefiles],
#         'targets':target,
#         'uptodate':[True],
#         'clean':True,
#     } 
    
# def task_copy_calfiles():
#     def movecalfile(dependencies, targets):
        
#         cams = pd.read_csv(dependencies[0]).groupby(by=['Operation','CameraSerialNumber']).first().reset_index()
#         for index,row in cams.iterrows():
#             source =os.path.join(config.basepath,config.cfg['calibration']['camfile'])
#             dest = os.path.basename(source.format(BarNumber=row.BarNumber,Position = row.Position,CameraSerialNumber = row.CameraSerialNumber))
#             dest = os.path.abspath(os.path.join(config.cfg['paths']['destpath'],f"{row['FieldTrip']}_{row['Operation']:03}",dest))
#             if os.path.exists(source):
#                 shutil.copy(source,dest)          
            
                    
#     file_dep = [os.path.join(config.basepath,config.cfg['paths']['rename'])]
#     target =  [os.path.join(config.basepath,config.cfg['paths']['movelog'])]
#     return {
#         'file_dep':file_dep,
#         'actions':[movecalfile],
#         'uptodate':[run_once],
#         'clean':True,
#     }     
    
if __name__ == '__main__':
    import doit
    DOIT_CONFIG = {'check_file_uptodate': 'timestamp'}
    #print(globals())
    doit.run(globals())