import pandas as pd
from pymongo import MongoClient

import bson


client = MongoClient('mongodb://dgrewal:Ph0enix5290!@localhost:27017/')
db = client.single_cell_qc

collection = db.metrics


def get_count(query):
    print "no of matches:{}".format(collection.count_documents(query))

def get_results(query):
    for result in collection.find(query):
        print result



print "cell_calls"

query = {'cell_call':'C1'}
get_count(query)

query = {"total_mapped_reads_hmmcopy": { "$gt": 500 } }
get_count(query)