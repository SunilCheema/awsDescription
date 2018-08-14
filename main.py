import requests
import pandas as pd


def download_file(url):
    local_filename = url.split('/')[-1]

    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

    return local_filename


def createNewFile():
    with open("index.csv", 'r') as f:
        with open("updated_test.csv", 'w') as f1:
            for i in range(5):
                f.next()
                print 'done'
            for line in f:
                f1.write(line)

r = requests.get('https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/us-east-1/index.csv')

file = download_file('https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/us-east-1/index.csv')

createNewFile()

dataframe = pd.read_csv('updated_test.csv')


cols = dataframe.columns.tolist()
a, b = cols.index('SKU'), cols.index('Instance Type')
cols[b], cols[a] = cols[a], cols[b]

dataframe = dataframe[cols]
dataframe = dataframe.sort_values(by=['Instance Type'])
dataframe.to_csv('out.csv', index = False)
print(dataframe.head())