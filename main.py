from __future__ import print_function

import json
import pandas as pd
import requests
import urllib
import os
import csv
import pickle
from itertools import islice
from collections import OrderedDict
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    
    download_file('https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-west-1/index.csv')
    removeRows()
    dataframe = pd.read_csv('/tmp/updated_test.csv')
    dataframe = swapColumns(dataframe)
    dataframe = sortByInstanceType(dataframe)
    dataframe = removeData(dataframe)
    dataframe = removeColumns(dataframe)
    dataframe.to_csv('/tmp/outNew.csv', index=False)
    
    link = storeFile()
    print('Latest link = ' + link)
    print("the fileKey is: "+ os.environ["fileKey"])
    
    downloadResult = 'none'
    if os.environ["fileKey"] == '4':
        storeEnvVariable(link)
        return 'env variable reset'
        
    else:
        downloadResult = downloadStoredFile(os.environ["fileKey"])
        print('not equal to 4 ')
    
    print('downloadedResult method result = '+ downloadResult)
    
    storeEnvVariable(link)
    oldDataframe = pd.read_csv('/tmp/downloaded.csv')
    dataframe = dataframe.reset_index()
    oldDataframe = oldDataframe.reset_index()
    #print('could not get downloaded csv')
    #print(dataframe.head(3))
    #oldDataframe.at[0,'PricePerUnit'] = 300
    dataframe = dataframe.drop(oldDataframe.index[0])
    #print(dataframe.head(3))
    
    difference, deleted, changedPrice, newInstances = handleEvents(oldDataframe,dataframe)
    print(difference)
    
    deleted.to_csv('/tmp/deleted.csv', index=False)
    
    
    #email(difference)
    
    
    email()
    #dataframe2 = dataframe.copy()
    #dataframe2.at[3398,'PricePerUnit'] = 300
    #dataframe2 = dataframe2.drop(dataframe2.index[20])
    #print(dataframe2.head())
    #differences = findNewPrices(dataframe, dataframe2)
    #print(differences)
    #dataframe.to_csv('/tmp/outNew.csv', index=False)
    
    return ' '

#downloads spreadsheet containing AWS EC2 description
def download_file(url):
    fullfilename = os.path.join('/tmp', 'index.csv')
    urllib.urlretrieve(url, fullfilename)

def download_file2(url):
    local_filename = '/tmp/downloaded.csv'
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                #f.flush() commented by recommendation from J.F.Sebastian
    return local_filename
    
# Removes first 5 rows of the csv file that contains metadata
def removeRows():
    with open("/tmp/index.csv", 'r') as f:
        with open("/tmp/updated_test.csv", 'w') as f1:
            for i in range(5):
                f.next()

            for line in f:
                f1.write(line)

#Make instance type the first column in spreadsheet
def swapColumns(dataframe):
    cols = dataframe.columns.tolist()
    
    a, b = cols.index('SKU'), cols.index('Instance Type')
    cols[b], cols[a] = cols[a], cols[b]
    dataframe = dataframe[cols]
    return dataframe

#order the columns by instance type
def sortByInstanceType(dataframe):
    dataframe = dataframe.sort_values(by=['Instance Type'])

    return dataframe

#store the latest csv somewhere temporary (file.io)
def storeFile():
    url = 'https://file.io/?expires=1w'
    files = {'file': open('/tmp/outNew.csv', 'rb')}
    r = requests.post(url, files=files)

    # retrieve link
    d = json.loads(r.content)
    link = d['link']

    return link
    
def downloadStoredFile(link):
    
    if os.environ["fileKey"] == 4:
        return 'no detection possible, env variable reset (dwnstored file)'
    else:
        fullfilename = os.path.join('/tmp', 'downloaded.csv')
        urllib.urlretrieve(link, fullfilename)
        return 'attempted download using file.io'
        
def handleEvents(oldFile, newFile):
    if os.environ["fileKey"] == 4:
        print('env variable reset (handleEvents')
    else:
        differences, deleted, changedPrice, newInstances = findNewPrices(oldFile, newFile)
        return differences, deleted, changedPrice, newInstances 

#remove unnecssecary columns
def removeColumns(dataframe):
    dataframe = dataframe.drop(
        ['SKU', 'OfferTermCode', 'RateCode', 'EffectiveDate', 'StartingRange', 'EndingRange', 'LeaseContractLength',
         'PurchaseOption', 'OfferingClass', 'Product Family', 'serviceCode', 'Location Type', 'Current Generation',
         'Instance Family', 'Physical Processor', 'Clock Speed', 'Storage Media', 'Volume Type', 'Max Volume Size',
         'Max IOPS/volume', 'Max IOPS Burst Performance', 'Max throughput/volume', 'Provisioned', 'EBS Optimized',
         'Group', 'Group Description', 'Transfer Type', 'From Location', 'From Location Type', 'To Location',
         'To Location Type', 'usageType', 'operation', 'CapacityStatus', 'Dedicated EBS Throughput', 'ECU',
         'Elastic GPU Type', 'Enhanced Networking Supported', 'GPU', 'GPU Memory', 'Instance',
         'Instance Capacity - 10xlarge', 'Instance Capacity - 12xlarge', 'Instance Capacity - 16xlarge',
         'Instance Capacity - 18xlarge', 'Instance Capacity - 24xlarge', 'Instance Capacity - 2xlarge',
         'Instance Capacity - 32xlarge', 'Instance Capacity - 4xlarge', 'Instance Capacity - 8xlarge',
         'Instance Capacity - 9xlarge', 'Instance Capacity - large', 'Instance Capacity - medium',
         'Instance Capacity - xlarge', 'Intel AVX Available', 'Intel AVX2 Available', 'Intel Turbo Available',
         'Normalization Size Factor', 'Physical Cores', 'Processor Features', 'serviceName',
         'TermType', 'Tenancy', 'License Model', 'Pre Installed S/W', 'PriceDescription'], axis=1)
    return dataframe

#Remove irrelevant rows
def removeData(dataframe):
    dataframe = dataframe[dataframe.TermType == 'OnDemand']
    dataframe = dataframe[dataframe.Tenancy == 'Shared']
    dataframe = dataframe[dataframe['Operating System'] != 'Linux']
    dataframe = dataframe[dataframe['License Model'] != 'Bring your own license']
    dataframe = dataframe[dataframe['Pre Installed S/W'].isnull()]
    return dataframe

#Find price change between two csv
def findNewPrices(pastFile, currentFile):
    pastInstanceList = pastFile['Instance Type'].tolist()
    pastOsList = pastFile['Operating System'].tolist()
    pastPriceList = pastFile['PricePerUnit'].tolist()

    presentInstanceList = currentFile['Instance Type'].tolist()
    presentOsList = currentFile['Operating System'].tolist()
    presentPriceList = currentFile['PricePerUnit'].tolist()

    instanceDictPast = OrderedDict()
    instanceDictPresent = OrderedDict()

    # print(pastInstanceList)
    # print(len(pastInstanceList))

    for i in range(len(pastInstanceList)):
        # listToPrint.append(i)
        instanceDictPast[pastInstanceList[i] + ' ' + pastOsList[i]] = pastPriceList[i]
    for i in range(len(presentInstanceList)):
        instanceDictPresent[presentInstanceList[i] + ' ' + presentOsList[i]] = presentPriceList[i]

    differences = []
    indexPast = -1
    indexCurrent = -1
    colNames = list(pastFile.columns.values)
    deletedValuesFrame = pd.DataFrame(columns = colNames)
    changedPricesFrame = pd.DataFrame(columns = colNames)
    newInstancesFrame = pd.DataFrame(columns = colNames)
    
    for key in instanceDictPast:
        indexPast += 1
        try:
            if abs(instanceDictPast[key]-instanceDictPresent[key])<0.00000001:
            #if float(instanceDictPast[key]) == float(instanceDictPresent[key]):
                y = 4

            else:
                instance, os = key.split(' ')
                result = 'change= instance: ' + instance + ',' + ' OS: ' + os + ',' + ' new price: ' + '$' + str(
                    instanceDictPresent[key]) + ' per hour'
                differences.append(result)
                print('past: '+str(instanceDictPast[key]))
                print('current: '+str(instanceDictPresent[key]))
                print(abs(instanceDictPast[key]-instanceDictPresent[key])<0.00000001)
                differentPriceRow = currentFile.iloc[indexPast]
                changedPricesFrame = changedPricesFrame.append(differentPriceRow)
        except:
            differences.append("Deleted value " + key)
            #print(list(islice(instanceDictPast, 10)))
            print(pastFile.head(3))
            deletedValueRow = pastFile.iloc[indexPast]
            
            ilocIndex = len(deletedValuesFrame.index)
            deletedValuesFrame = deletedValuesFrame.append(deletedValueRow)
            
            #deletedValuesFrame.iloc[ilocIndex+1] = deletedValueRow
            #print(deletedValuesFrame)
            #print(deletedValueRow)
            #print(pastInstanceList[1])
            #print(deletedValuesDataframe)
            #print(deletedValueDatafram)
            #print("New or deleted value")
    for key in instanceDictPresent:
        indexCurrent += 1
        if key not in instanceDictPast:
            differences.append("Added value " + key)
            newValueRow = currentFile.iloc[indexCurrent]
            newInstancesFrame = newInstancesFrame.append(newValueRow)
    # print(differences)
    return differences, deletedValuesFrame, changedPricesFrame, newInstancesFrame


def storeEnvVariable(link):
    client = boto3.client('lambda')
    response = client.update_function_configuration(
            FunctionName='cloud9-awsDescription2-awsDescription2-6G7KIDQALZ4Y',
            Environment={
                'Variables': {
                    'fileKey': link
                }
            }
        )

def email():
    ses = boto3.client('ses')
    msg = MIMEMultipart()
    msg['Subject'] = 'weekly report'
    msg['From'] =  "sunil03cheema@hotmail.co.uk"
    msg['To'] = "sunil03cheema@hotmail.co.uk"

    # what a recipient sees if they don't use an email reader
    msg.preamble = 'Multipart message.\n'

    # the message body
    part = MIMEText('Recent EC2 changes')
    msg.attach(part)

    # the attachment
    part = MIMEApplication(open('/tmp/deleted.csv', 'rb').read())
    part.add_header('Content-Disposition', 'attachment', filename='removed_instances.csv')
    msg.attach(part)
    
    result = ses.send_raw_email(RawMessage={
                       'Data': msg.as_string(),
                   }, 
                   Source=msg['From'], 
                   Destinations=['sunil03cheema@hotmail.co.uk']
                   )
