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
import dask
import dask.dataframe as dd
import shlex
import ast

def task_exif_data():
    def get_fullexif(dependencies, targets):
            os.makedirs(os.path.dirname(targets[0]),exist_ok=True)
            command = f"exiftool -api largefilesupport=1 -u  -json -ext MP4 -q -CameraSerialNumber -CreateDate -SourceFile -Duration -FileSize -FieldOfView {dependencies[0]}"
            process = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, err = process.communicate()
            df =pd.read_json(out)
            df.to_csv(targets[0],index=False)



    gopro = glob.glob(f"{config.geturl('cardstore')}/**/100GOPRO",recursive=True)
    files = pd.DataFrame(gopro,columns=['Directory'])
    files['id'] = files.Directory.str.extract(r'(?P<id>[RL]\d\d_C[0-9]{13}_[0-9]{8}T[0-9]{4})')
    files = files[~files['id'].isna()]
    for index,row in files.iterrows():
        if glob.glob(f"{row.Directory}/*.MP4"):
            target = f"{config.geturl('process')}/{row.id}.csv"
            yield {
                'name' : target,
                'file_dep': [row.Directory],
                'actions': [get_fullexif],
                'targets': [target],
                'uptodate': [True],
                'clean':True
            }

def task_report_data():
    def make_report(dependencies, targets):
         data = pd.concat([ pd.read_csv(file) for file in dependencies])
         data['Directory'] = data['SourceFile'].apply(os.path.dirname)
         data['id'] = data.Directory.str.extract(r'(?P<id>[RL]\d\d_C[0-9]{13}_[0-9]{8}T[0-9]{4})')
         data =data.groupby('id').first().reset_index()
         data['DateStamp'] =pd.to_datetime(data.id.str.split('_').apply(lambda x: x[-1]))
         data = data.sort_values('DateStamp')
         data.to_csv(targets[0],index=False)
    gopro = glob.glob(f"{config.geturl('process')}/*.csv",recursive=True)
    os.makedirs(config.geturl('report'),exist_ok=True)
    target = f"{config.geturl('report')}/report.csv"
    return {
        'file_dep':gopro,
        'actions':[make_report],
        'targets':[target],
        'uptodate':[False],
        'clean':True,
    }
if __name__ == '__main__':
    import doit
    DOIT_CONFIG = {'check_file_uptodate': 'timestamp'}
    #print(globals())
    doit.run(globals())
