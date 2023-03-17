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
from task_exif import task_init
from config import task_read_config
from task_exif import task_create_json
from task_exif import task_concat_json


@create_after(executed='concat_json', target_regex='.*\.json')  
def task_extract_times():
    def calcnames(dependencies, targets):
        def cluster(df,timedelta='7200s'):
            df['CamGroup']=0
            df.loc[(df.StartTime.shift(-1)-df.EndTime) > pd.Timedelta(timedelta),'CamGroup']=1
            df['CamGroup']=df['CamGroup'].cumsum()
            return df
        exif = pd.read_csv(dependencies[0])
        # clean up bad files
        exif =exif.loc[~exif.Duration.isna()]
        parts =exif['FileName'].apply(lambda x:os.path.splitext(x)[0]).str.split('_',expand=True)
        parts.iloc[parts[1]=='Port',1] = 'LFT'
        parts.iloc[parts[1]=='STRB',1] = 'RHT'
        exif['CAM'] = parts[1]
        exif['CamGroup']=0
        exif['Instrument'] = parts[0].str.upper()
        parts[0] = parts[0].str.upper()
        exif['StartTime']=pd.to_datetime(parts[4]+parts[3]+parts[2]+'T'+parts[5]+parts[6]+parts[7])
        exif['EndTime'] = exif['StartTime'] + pd.to_timedelta(exif.Duration)
        # exif['NewName'] = exif.apply(lambda x: f"{x.Instrument}_{x.CAM}_{config.cfg['processing']['voyage']}_{x['StartTime'].strftime('%Y%m%dT%H%M%S')}.MP4", axis=1)
        # exif['Output'] = exif.apply(lambda x: os.path.join(config.basepath,config.cfg['paths']['output'],x['Instrument'],x['NewName']),axis=1) 
        exif =exif.groupby(['Instrument','CAM']).apply(cluster)
        exif['CamGroup']=exif.apply(lambda x:f"{x.Instrument}_{x.CAM}_{x.CamGroup:03}",axis=1)
        exif[['SourceFile', 'FileName', 'Duration','RelPath', 'CAM', 'CamGroup', 'Instrument', 'StartTime','EndTime']].sort_values('RelPath').to_csv(targets[0],index=False)
    file_dep = os.path.join(config.basepath,config.cfg['paths']['process'],config.cfg['paths']['exifcat'])
    target =  os.path.join(config.basepath,config.cfg['paths']['process'],config.cfg['paths']['timename'])
    return {
        'file_dep':[file_dep],
        'actions':[calcnames],
        'targets':[target],
        'uptodate':[True],
        'clean':True,
    }


@create_after(executed='extract_times', target_regex='.*\.json')  
def task_process_summary():
    def process_summary(dependencies, targets):
        def process_cast(df):
            df['PairNumber'] =df.groupby('CorrectedStartTime').ngroup()
            return df
        data = pd.read_csv(dependencies[0],parse_dates=['StartTime','EndTime','TimeOffset','Duration'])
        data = data.loc[~data['Station'].isna()]
        data.TimeOffset =pd.to_timedelta(data.TimeOffset)
        data['CorrectedStartTime'] =data.StartTime +data.TimeOffset
        data['PairNumber']=0
        data['Operation']= data['Operation'].astype(int32)
        data.loc[data.Station.str.isnumeric(),'Station']=data.loc[data.Station.str.isnumeric(),'Station'].apply(lambda x: f'{pd.to_numeric(x):03}')
        data =data.groupby(['Instrument','Operation']).apply(process_cast)
        data['NewName'] = data.apply(lambda x: f"{x.Instrument}_{x.CAM}_{config.cfg['processing']['voyage']}_{x['Station']}_{x['CorrectedStartTime'].strftime('%Y%m%dT%H%M%S')}_{x['Operation']:03}_{x['PairNumber']:03}.MP4", axis=1)
        data['Output'] = data.apply(lambda x: os.path.join(config.basepath,config.cfg['paths']['output'],f"Station_{x['Station']}",f"{x['Instrument']}_{x['Operation']:03}",x['NewName']),axis=1) 
        data.to_csv(targets[0],index=False)
    file_dep = os.path.join(config.basepath,config.cfg['paths']['process'],config.cfg['paths']['sumname'])
    target = os.path.join(config.basepath,config.cfg['paths']['process'],config.cfg['paths']['finname'])         
    return {
        'file_dep':[file_dep],
        'actions':[process_summary],
        'targets':[target],
        'uptodate':[True],
        'clean':True,
    }

@create_after(executed='process_summary', target_regex='.*\.csv')  
def task_set_times():
    timefile = os.path.join(config.basepath,config.cfg['paths']['process'],config.cfg['paths']['finname'])
    if os.path.exists(timefile):
        files = pd.read_csv(timefile,parse_dates=['StartTime'])
        files = files.loc[~files['Station'].isna()]
        for index,row in files.iterrows():
            if not os.path.exists(row.Output):
                yield {
                    'name':row['NewName'], 
                    'file_dep':[row['SourceFile']],
                    'actions':[f"exiftool -api largefilesupport=1 \"-alldates={row['StartTime']}\" -VideoFrameRate=30 -o {row['Output']} {row['SourceFile']}"],
                    'targets':[row['Output']],
                    'uptodate':[True],
                    'clean':True,
                }      

if __name__ == '__main__':
    import doit
    DOIT_CONFIG = {'check_file_uptodate': 'timestamp'}
    #print(globals())
    doit.run(globals())

#exiftool "-alldates=2022:11:20 10:02:45" DeepBRUVS2_Port_20_11_2022_10_02_45.mp4