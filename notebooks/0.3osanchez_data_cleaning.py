import os
import pandas as pd
from pathlib import Path
from fileinput import filename
    
root_dir=Path ('.').resolve().parent
root_dir
file_name='llamadas123_julio_2022.csv'
file_path=os.path.join(root_dir,"data","raw",file_name)
file_path
df=pd.read_csv(file_path,sep=";",encoding="latin-1")
df.head(10)