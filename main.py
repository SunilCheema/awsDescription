from __future__ import print_function

import json
import pandas as pd
import requests
import urllib
import os

print('Loading function')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    download_file('https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-west-1/index.csv')

    createNewFile()

    dataframe = pd.read_csv('/tmp/updated_test.csv')

    #swaps the column positions to put instance type first
    cols = dataframe.columns.tolist()
    a, b = cols.index('SKU'), cols.index('Instance Type')
    cols[b], cols[a] = cols[a], cols[b]

    #sorts the dataframe according to the instance type column
    dataframe = dataframe[cols]
    dataframe = dataframe.sort_values(by=['Instance Type'])

    #Exports the dataframe as a csv file
    dataframe.to_csv('/tmp/out.csv', index = False)
    print(dataframe.head())
    return ' '  # Echo back the first key value
    #raise Exception('Something went wrong')

def download_file(url):
    fullfilename = os.path.join('/tmp', 'index.csv')
    urllib.urlretrieve(url, fullfilename)

# Removes first 5 rows of the csv file
def createNewFile():
    with open("/tmp/index.csv", 'r') as f:
        with open("/tmp/updated_test.csv", 'w') as f1:
            for i in range(5):
                f.next()
                
            for line in f:
                f1.write(line)

