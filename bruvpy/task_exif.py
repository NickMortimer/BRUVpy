import doit
from doit import get_var
from doit.tools import run_once
from doit import create_after
import glob
import yaml
import os
import pandas as pd
import shutil

cfg = None
basepath = None

def task_read_config():
    global cfg
    global basepath
    config = {"config": get_var('config', 'NO')}
    with open(config['config'], 'r') as ymlfile:
        cfg = yaml.load(ymlfile, yaml.SafeLoader)
    basepath = os.path.dirname(config['config'])



def task_create_videolist():
    def scan_videos(dependencies, targets,videosource):
        basepath = os.path.dirname(targets[0])
        videos = glob.glob(videosource,recursive=True)
        if videos:
            df =pd.DataFrame(videos,columns=['FileName'])
            output =pd.DataFrame(df.apply(lambda x: os.path.split(os.path.relpath(x['FileName'],basepath)),axis=1).tolist(),index=df.index,columns=['RelName','FileName'])            
            output['FullPath'] = df['FileName']
            output.to_csv(targets[0],index=False)

    target =  os.path.join(basepath,cfg['paths']['videolist'])
    os.makedirs(os.path.dirname(target),exist_ok=True)           
    return {
        'actions':[(scan_videos, [],{'videosource':os.path.join(basepath,cfg['paths']['videosource'],cfg['paths']['videowild'])})],
        'targets':[target],
        'uptodate':[False],
        'clean':True,
    }
        

 
@create_after(executed='create_videolist', target_regex='.*\.csv')   
def task_create_json():
    global basepath,cfg
    exifpath = os.path.abspath(os.path.join(basepath,cfg['paths']['exiftool']))
    videolist =  os.path.join(basepath,cfg['paths']['videolist'])
    vbasepath = os.path.dirname(videolist)
    if os.path.exists(videolist):
        video = pd.read_csv(videolist)
        paths =video.groupby('RelName')
        for name,df in paths:
            source = os.path.abspath(os.path.join(vbasepath,name))
            filter = os.path.join(source,cfg['paths']['videowild'])
            target = os.path.join(source,cfg['paths']['exifname'])
            yield {
                'name':name,
                'actions':[f'"{exifpath}" -api largefile=1 -u -json "{filter}" > "{target}"'],
                'targets':[target],
                'uptodate':[run_once],
                'clean':True,
            }
@create_after(executed='create_json', target_regex='.*\.json')                   
def task_concat_json():
    def concatfiles(dependencies, targets):
        def open_exif(file):
            df = pd.read_json(file)
            df['CameraSerialNumber'] = df['CameraSerialNumber'].ffill().bfill()
            df['Duration'] = df['Duration'].ffill().bfill()
            return df
            
        exbasepath = os.path.dirname(targets[0])
        exif = pd.concat([open_exif(file) for file in dependencies])
        exif['RelPath'] = exif.apply(lambda x: os.path.relpath(x['SourceFile'],exbasepath),axis=1)
        exif.to_csv(targets[0],index=False)
        pass
    file_dep = glob.glob( os.path.abspath(os.path.join(basepath,cfg['paths']['videosource'],cfg['paths']['exifname'])),recursive=True)
    target =  os.path.join(basepath,cfg['paths']['exiflist'])
    return {
        'file_dep':file_dep,
        'actions':[concatfiles],
        'targets':[target],
        'uptodate':[True],
        'clean':True,
    }
    
def task_match_cameras():
    def matchcameras(dependencies, targets):
        exifdf = pd.read_csv(os.path.join(basepath,cfg['paths']['exiflist']),index_col='CameraSerialNumber')
        exifdf = exifdf[exifdf.columns[exifdf.columns.isin(cfg['exifcols'])]]
        exifdf['EndTime'] = pd.to_datetime(exifdf.FileModifyDate,format='%Y:%m:%d  %H:%M:%S%z')
        exifdf['StartTime'] =exifdf['EndTime'] - pd.to_timedelta(exifdf.Duration)
        bruv = pd.read_csv(os.path.join(basepath,cfg['paths']['bruvconfig']),index_col='CameraSerialNumber')
        bruv=exifdf.join(bruv)
        bruv.to_csv(targets[0],index=True)
        pass
    file_dep = [os.path.join(basepath,cfg['paths']['exiflist']),os.path.join(basepath,cfg['paths']['bruvconfig'])]
    target =  os.path.join(basepath,cfg['paths']['camlist'])
    return {
        'file_dep':file_dep,
        'actions':[matchcameras],
        'targets':[target],
        'uptodate':[True],
        'clean':True,
    }    
    
def task_report_cameras():
    def camreport(dependencies, targets):
        def processframe(df):
            leftstart = df[df.Position=='LFT'].StartTime.min()
            rightstart = df[df.Position=='RGT'].StartTime.min()
            # Right is behind left
            if (leftstart - rightstart).total_seconds()/60 >20:
                df.loc[df.Position=='RGT','StartTime'] = df.loc[df.Position=='RGT','StartTime'] +(leftstart - rightstart)
                df.loc[df.Position=='RGT','EndTime'] = df.loc[df.Position=='RGT','EndTime'] +(leftstart - rightstart)
            elif (leftstart - rightstart).total_seconds()/60 <-20:
                df.loc[df.Position=='LFT','StartTime'] = df.loc[df.Position=='LFT','StartTime'] -(leftstart - rightstart)
                df.loc[df.Position=='LFT','EndTime'] = df.loc[df.Position=='LFT','EndTime'] -(leftstart - rightstart)
            
            return df

        camlist = pd.read_csv(dependencies[0],parse_dates=['StartTime','EndTime','Duration'])
        camlist =camlist.groupby('BarNumber').apply(processframe)
        camlist.to_csv(targets[0],index=False)
        pass
    file_dep = [os.path.join(basepath,cfg['paths']['camlist'])]
    target =  [os.path.join(basepath,cfg['paths']['camreport'])]
    return {
        'file_dep':file_dep,
        'actions':[camreport],
        'targets':target,
        'uptodate':[False],
        'clean':True,
    } 

def task_calc_rename():
    def calcrename(dependencies, targets):
        def calc_rename(df,cams):
            operation = cams[(cams.StartTime>df.StartTime-pd.to_timedelta('5min')) & (cams.EndTime<df.EndTime+pd.to_timedelta('5min')) & (df.BarNumber== cams.BarNumber) ].copy().sort_values('StartTime')
            operation['Operation'] = df.Operation
            operation['FieldTrip'] = df.FieldTrip
            operation['FileNumber'] =1
            operation.loc[operation.Position=='RGT','FileNumber']=operation.loc[operation.Position=='RGT','FileNumber'].cumsum()
            operation.loc[operation.Position=='LFT','FileNumber']=operation.loc[operation.Position=='LFT','FileNumber'].cumsum()
            operation['ProposedName'] = operation.apply(lambda x: f"LRUVS_{x['Position']}_{x['FieldTrip']}_{x['Operation']:03}_{x['StartTime'].strftime('%Y%m%dT%H%M%S')}_{x['FileNumber']:03}",axis=1)
            return operation
        # in calibration mode
        cams = pd.read_csv(os.path.join(basepath,cfg['paths']['camreport']),parse_dates=['StartTime','EndTime']).sort_values('StartTime')
        if 'calibration' in cfg:
            operation = cams
            operation['Operation'] = operation['BarNumber']
            operation['FieldTrip'] = cfg['calibration']['name']
            operation['FileNumber'] =1
            operation['FileNumber']=operation.groupby(by=['BarNumber','Position'])['FileNumber'].cumsum()              
            operation['ProposedName'] = operation.apply(lambda x: f"LRUVS_{x['Position']}_{x['FieldTrip']}_{x['Operation']:03}_{x['StartTime'].strftime('%Y%m%dT%H%M%S')}_{x['FileNumber']:03}",axis=1)
            operation.to_csv(targets[0],index=False)
        else:                
            trips = pd.read_csv(os.path.join(basepath,cfg['paths']['tripsheet']),parse_dates=['StartTime','EndTime'])
            matched = pd.concat(list(trips.apply(calc_rename,cams=cams,axis=1)))
            matched.to_csv(targets[0],index=False)
    if 'calibration' in cfg:
        file_dep = [os.path.join(basepath,cfg['paths']['camreport'])]
    else:
        file_dep = [os.path.join(basepath,cfg['paths']['tripsheet']),os.path.join(basepath,cfg['paths']['camreport'])]
    target =  [os.path.join(basepath,cfg['paths']['rename'])]
    return {
        'file_dep':file_dep,
        'actions':[calcrename],
        'targets':target,
        'uptodate':[False],
        'clean':True,
    } 
    

def task_mov_files():
    def movefiles(dependencies, targets):
        files = pd.read_csv(dependencies[0])
        path = os.path.dirname(dependencies[0])
        with open(targets[0], 'w') as f:
            for index,row in files.iterrows():
                source = os.path.abspath(os.path.join(path,row['RelPath']))
                dest = os.path.abspath(os.path.join(cfg['paths']['destpath'],f"{row['FieldTrip']}_{row['Operation']:03}",row['ProposedName']+os.path.splitext(row['FileName'])[1]))
                os.makedirs(os.path.dirname(dest),exist_ok=True)
                f.write(f'move {source} -> {dest}')
                if os.path.exists(source):
                    shutil.move(source,dest)
            
            
 
    file_dep = [os.path.join(basepath,cfg['paths']['rename'])]
    target =  [os.path.join(basepath,cfg['paths']['movelog'])]
    return {
        'file_dep':file_dep,
        'actions':[movefiles],
        'targets':target,
        'uptodate':[True],
        'clean':True,
    } 
    
def task_copy_calfiles():
    def movecalfile(dependencies, targets):
        
        cams = pd.read_csv(dependencies[0]).groupby(by=['Operation','CameraSerialNumber']).first().reset_index()
        for index,row in cams.iterrows():
            source =os.path.join(basepath,cfg['calibration']['camfile'])
            dest = os.path.basename(source.format(BarNumber=row.BarNumber,Position = row.Position,CameraSerialNumber = row.CameraSerialNumber))
            dest = os.path.abspath(os.path.join(cfg['paths']['destpath'],f"{row['FieldTrip']}_{row['Operation']:03}",dest))
            if os.path.exists(source):
                shutil.copy(source,dest)          
            
                    
    file_dep = [os.path.join(basepath,cfg['paths']['rename'])]
    target =  [os.path.join(basepath,cfg['paths']['movelog'])]
    return {
        'file_dep':file_dep,
        'actions':[movecalfile],
        'uptodate':[run_once],
        'clean':True,
    }     
    
if __name__ == '__main__':
    import doit
    DOIT_CONFIG = {'check_file_uptodate': 'timestamp'}
    #print(globals())
    doit.run(globals())