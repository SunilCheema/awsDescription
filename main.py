from __future__ import print_function

import json
import pandas as pd
import requests
import urllib
import os
import csv
import pickle


def lambda_handler():

    download_file('https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-west-1/index.csv')
    removeRows()
    dataframe = pd.read_csv('/tmp/updated_test.csv')
    dataframe = swapColumns(dataframe)
    dataframe = sortByInstanceType(dataframe)
    dataframe = removeColumns(dataframe)
    dataframe = removeData(dataframe)
    dataframe = removeMoreColumns(dataframe)
    dataframe.to_csv('/tmp/outNew.csv', index=False)


    return ' '


def download_file(url):
    fullfilename = os.path.join('/tmp', 'index.csv')
    urllib.urlretrieve(url, fullfilename)


# Removes first 5 rows of the csv file
def removeRows():
    with open("/tmp/index.csv", 'r') as f:
        with open("/tmp/updated_test.csv", 'w') as f1:
            for i in range(5):
                f.next()

            for line in f:
                f1.write(line)


def swapColumns(dataframe):
    cols = dataframe.columns.tolist()
    print(cols)
    a, b = cols.index('SKU'), cols.index('Instance Type')
    cols[b], cols[a] = cols[a], cols[b]
    dataframe = dataframe[cols]
    return dataframe

def sortByInstanceType(dataframe):
    dataframe = dataframe.sort_values(by=['Instance Type'])

    return dataframe

def storeFile():
    url = 'https://file.io/?expires=1w'
    files = {'file': open('/tmp/out.csv', 'rb')}
    r = requests.post(url, files=files)

    # retrieve link
    d = json.loads(r.content)
    link = d['link']

    print(link)

def removeColumns(dataframe):
    dataframe = dataframe.drop(['SKU', 'OfferTermCode', 'RateCode', 'EffectiveDate', 'StartingRange', 'EndingRange', 'LeaseContractLength', 'PurchaseOption', 'OfferingClass', 'Product Family', 'serviceCode', 'Location Type', 'Current Generation', 'Instance Family', 'Physical Processor', 'Clock Speed', 'Storage Media', 'Volume Type', 'Max Volume Size', 'Max IOPS/volume', 'Max IOPS Burst Performance', 'Max throughput/volume', 'Provisioned', 'EBS Optimized', 'Group', 'Group Description', 'Transfer Type', 'From Location', 'From Location Type', 'To Location', 'To Location Type', 'usageType', 'operation', 'CapacityStatus', 'Dedicated EBS Throughput', 'ECU', 'Elastic GPU Type', 'Enhanced Networking Supported', 'GPU', 'GPU Memory', 'Instance', 'Instance Capacity - 10xlarge', 'Instance Capacity - 12xlarge', 'Instance Capacity - 16xlarge', 'Instance Capacity - 18xlarge', 'Instance Capacity - 24xlarge', 'Instance Capacity - 2xlarge', 'Instance Capacity - 32xlarge', 'Instance Capacity - 4xlarge', 'Instance Capacity - 8xlarge', 'Instance Capacity - 9xlarge', 'Instance Capacity - large', 'Instance Capacity - medium', 'Instance Capacity - xlarge', 'Intel AVX Available', 'Intel AVX2 Available', 'Intel Turbo Available', 'Normalization Size Factor', 'Physical Cores', 'Processor Features', 'serviceName'], axis=1)
    return dataframe

def removeData(dataframe):
    dataframe = dataframe[dataframe.TermType == 'OnDemand']
    dataframe = dataframe[dataframe.Tenancy == 'Shared']
    dataframe = dataframe[dataframe['Operating System'] != 'Linux']
    dataframe = dataframe[dataframe['License Model'] != 'Bring your own license']
    dataframe = dataframe[dataframe['Pre Installed S/W'].isnull()]
    return dataframe

def removeMoreColumns(dataframe):
    dataframe = dataframe.drop(['TermType', 'Tenancy', 'License Model', 'Pre Installed S/W', 'PriceDescription'],axis=1)
    return dataframe

def testInstances():
    old = ['t4', 'y5','u6','lol']
    present = ['t4','y5','u6','i7']
    
    newInstances = list(set(present) - set(old))
    oldInstances = list(set(old) - set(present)) 
    
    print(newInstances)
    print(oldInstances)
    

def findNewPrices(pastFile, currentFile):
    pastInstanceList = pastFile['Instance Type'].tolist()
    pastOsList = pastFile['Operating System'].tolist()
    pastPriceList = pastFile['PricePerUnit'].tolist()

    presentInstanceList = currentFile['Instance Type'].tolist()
    presentOsList = currentFile['Operating System'].tolist()
    presentPriceList = currentFile['PricePerUnit'].tolist()
    
    instanceDictPast = {}
    instanceDictPresent = {}    
    
    #print(pastInstanceList)
    #print(len(pastInstanceList))
    
    for i in range(len(pastInstanceList)):  
        
        #listToPrint.append(i)
        instanceDictPast[pastInstanceList[i]+' '+pastOsList[i]] = pastPriceList[i]
        instanceDictPresent[presentInstanceList[i]+' '+presentOsList[i]] = presentPriceList[i]
    
    differences = []
 
    for key in instanceDictPast:
        try:
            if instanceDictPast[key] == instanceDictPresent[key]:
                y = 4
                
            else:
                instance, os = key.split(' ')
                result = 'change= instance: ' + instance + ',' + ' OS: ' + os + ',' + ' new price: ' +'$'+ str(instanceDictPresent[key]) + ' per hour'
                differences.append(result)
        except:
            print("New or deleted value")
    
    #print(differences)
    return differences

lambda_handler()
