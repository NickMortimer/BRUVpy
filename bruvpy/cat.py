import pandas as pd


data = pd.concat([pd.read_json("~/bruvlist.json"),pd.read_json("~/bruvlistFBackup1.json")])