import pandas as pd
from pymongo import MongoClient

import bson


client = MongoClient('mongodb://dgrewal:Ph0enix5290!@localhost:27017/')
db = client.single_cell_qc

collection = db.metrics

df = pd.read_csv('A96224B_multiplier0_metrics.csv.gz')



for i,row in df.iterrows():
    row = row.to_dict()
    #row = bson.BSON.encode(row)
    print collection.insert_one(row).inserted_id