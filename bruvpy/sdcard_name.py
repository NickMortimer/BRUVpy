import subprocess
from io import BytesIO
import pandas as pd

def run(cmd):
    completed = subprocess.run(["powershell", "-Command", cmd], capture_output=True)
    return completed

def rename_drives(x):
    cmd =f'Set-Volume -DriveLetter {x.DriveLetter[0]} -NewFileSystemLabel "{x.NewName}"'
    print(cmd)
    run(cmd)


cmd ='Get-CimInstance -ClassName Win32_Volume|  ? { $_.DriveType -eq 2 }  | Select Name, DriveLetter, DriveType, Capacity, serialnumber | ConvertTo-Json'
output =run(cmd)



df = pd.read_json(BytesIO(output.stdout))    
df = df.dropna(subset=['serialnumber'])
df['hex']=df.serialnumber.astype(int).apply(lambda x: f'{x:08x}').apply(lambda x: f'{x[0:4]}-{x[4:10]}')
df['NewName']=df.serialnumber.astype(int).apply(lambda x: f'{x:08x}').apply(lambda x: f'BV{x[0:4]}-{x[4:10]}')
for index,row in df.iterrows():
    rename_drives(row)
