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
    backupsessions = config.geturl('backupsessions')
    if os.path.exists(backupsessions):
        backups = pd.read_csv(backupsessions)