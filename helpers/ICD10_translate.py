import json
import pandas as pd

with open('ICD_10.json') as json_data:
    ICD10_dict = json.load(json_data)

def translate(col):
    return col.replace(ICD_dict)
