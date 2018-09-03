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
from tabulate import tabulate

IRELANDKEY = '4'
LONDONKEY = '4'
allDifferences= ''

def lambda_handler(event, context):
    
    try:
        awsUrlIreland = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-west-1/index.csv'
        environmentVariableIreland = "fileKey"
        irelandResults = findDifferences(awsUrlIreland, environmentVariableIreland)
    except:
        print('failed Ireland')
    
    try:
        awsUrlLondon = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-west-2/index.csv'
        environmentVariableLondon = "londonKey"
        londonResults = findDifferences(awsUrlLondon, environmentVariableLondon)
    except:
        print('failed London')
        
    try:
        print('Irelend key: ' + IRELANDKEY)
        print('London key: ' + LONDONKEY)
        storeEnvVariable()
    except:
        print('failed to store variables')
    
    combinedResults = irelandResults + londonResults
    result = pd.concat(combinedResults, sort=False)
    #result = result.drop('index',1)
    result = result.drop('index',1)
    #result = result.drop(' ',1)
    result = result.drop(result.columns[1], axis=1)
    
    result.to_csv('/tmp/result.csv')
    fname_in = '/tmp/result.csv'
    fname_out = '/tmp/result2.csv'
    with open(fname_in, 'rb') as fin, open(fname_out, 'wb') as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        for row in reader:
            writer.writerow(row[1:])
    #result.to_csv('/tmp/result.csv')
    email()
    return ' '

def findDifferences(awsUrl, environmentVariable):
    download_file(awsUrl)
    removeRows()
    dataframe = pd.read_csv('/tmp/updated_test.csv')
    dataframe = swapColumns(dataframe)
    dataframe = sortByInstanceType(dataframe)
    dataframe = removeData(dataframe)
    dataframe = removeColumns(dataframe)
    dataframe.to_csv('/tmp/outNew.csv', index=False)
    
    link = storeFile()
    print('Latest link = ' + link)
    print("the fileKey is: "+ os.environ[environmentVariable])
    
    if awsUrl == 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-west-1/index.csv':
        global IRELANDKEY
        IRELANDKEY = link
        print('IRELAND INVOKED')
        print(IRELANDKEY)
    else:
        global LONDONKEY 
        LONDONKEY = link
        print('LONDON ENVOKED')
        print(LONDONKEY)
    
    downloadResult = 'none'
    if os.environ[environmentVariable] == '4':
        #storeEnvVariable(link, environmentVariable)
        #storeEnvVariable()
        return 'env variable reset'
        
    else:
        downloadResult = downloadStoredFile(os.environ[environmentVariable],environmentVariable)
        print('not equal to 4 ')
    
    print('downloadedResult method result = '+ downloadResult)
    
    #storeEnvVariable()
    #storeEnvVariable(link, environmentVariable)
    oldDataframe = pd.read_csv('/tmp/downloaded.csv')
    dataframe = dataframe.reset_index()
    oldDataframe = oldDataframe.reset_index()
    dataframe = dataframe.drop(dataframe.index[0])
    oldDataframe = oldDataframe.drop(oldDataframe.index[7])
    difference, deleted, changedPrice, newInstances = handleEvents(oldDataframe,dataframe, environmentVariable)
    print(difference)

    
    deleted = deleted.drop('index', 1)
    deleted.to_csv('/tmp/deleted.csv', index=False)
    
    allDataframes = [deleted, changedPrice, newInstances]
    dataframeNames = ['deleted added', 'changedPrice added', 'newInstances added']
    dataframesToReturn = []
    
    index = -1
    for frame in allDataframes:
        index += 1
        if frame.shape[0] != 0:
            print(dataframeNames[index])
            dataframesToReturn.append(frame)
    
    #if deleted != 0 push through array of strings equal to the values in the dataframe
    #email(difference)
    
    
    #email()
    
    
    #dataframe2 = dataframe.copy()
    #dataframe2.at[3398,'PricePerUnit'] = 300
    #dataframe2 = dataframe2.drop(dataframe2.index[20])
    #print(dataframe2.head())
    #differences = findNewPrices(dataframe, dataframe2)
    #print(differences)
    #dataframe.to_csv('/tmp/outNew.csv', index=False)
    
    os.remove("/tmp/downloaded.csv")
    os.remove("/tmp/outNew.csv")
    os.remove("/tmp/updated_test.csv")
            
    return dataframesToReturn
    
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
    
def downloadStoredFile(link, envVariable):
    
    if os.environ[envVariable] == 4:
        return 'no detection possible, env variable reset (dwnstored file)'
    else:
        fullfilename = os.path.join('/tmp', 'downloaded.csv')
        urllib.urlretrieve(link, fullfilename)
        return 'attempted download using file.io'
        
def handleEvents(oldFile, newFile, envVariable):
    if os.environ[envVariable] == 4:
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
            #print(pastFile.head(3))
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


def storeEnvVariable():
    client = boto3.client('lambda')
    
    response = client.update_function_configuration(
            FunctionName='cloud9-awsDescription2-awsDescription2-6G7KIDQALZ4Y',
            Environment={
                'Variables': {
                    'fileKey': IRELANDKEY,
                    'londonKey': LONDONKEY
                }
            }
        )

def emailList(differences):
    result = """ """
    for diff in differences:
        result += result
    return result

def email():
    ses = boto3.client('ses')
    msg = MIMEMultipart()
    msg['Subject'] = 'weekly report'
    msg['From'] =  "sunil03cheema@hotmail.co.uk"
    msg['To'] = "sunil03cheema@hotmail.co.uk"

    # what a recipient sees if they don't use an email reader
    msg.preamble = 'Multipart message.\n'

    # the message body
    
    #part = MIMEText('Recent EC2 changes')
    #msg.attach(part)
    
    #if statement that attaches parts based on whether a variable is true
    
    text = """
    Hello, Friend.
    Here is your data:
    {table}
    Regards,
    Me"""

    html = """
    <html><body><p>Hello, Friend.</p>
    <p>Here is your data:</p>
    {content}
    <div style="font-size:11px">
    {table}
    </div>
    <p>Regards,</p>
    <p>Me</p>
    </body></html>
    """

    with open('/tmp/result2.csv') as input_file:
        reader = csv.reader(input_file)
        data = list(reader)
    
    
    text = text.format(table=tabulate(data, headers="firstrow", tablefmt="grid"))
    html = html.format(table=tabulate(data, headers="firstrow", tablefmt="html"), content = """ lol """)
    #html = html.format(content = ['first','second'])
    #html = html.format(table=data)
    #msg.attach(MIMEText(text))
    msg.attach(MIMEText(html,'html'))
    #msg.attach(MIMEText('lol'))
    
    # the attachment
    part = MIMEApplication(open('/tmp/result2.csv', 'rb').read())
    part.add_header('Content-Disposition', 'attachment', filename='removed_instances.csv')
    msg.attach(part)
    
    
    result = ses.send_raw_email(RawMessage={
                       'Data': msg.as_string(),
                   }, 
                   Source=msg['From'], 
                   Destinations=['sunil03cheema@hotmail.co.uk']
                   )
