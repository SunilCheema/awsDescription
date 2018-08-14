import requests
import pandas as pd

#downloads the file
def download_file(url):
    local_filename = url.split('/')[-1]

    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)

    return local_filename

# Removes first 5 rows of the csv file
def createNewFile():
    with open("index.csv", 'r') as f:
        with open("updated_test.csv", 'w') as f1:
            for i in range(5):
                f.next()
                print 'done'
            for line in f:
                f1.write(line)


file = download_file('https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/current/eu-west-1/index.csv')

createNewFile()

dataframe = pd.read_csv('updated_test.csv')

#swaps the column positions to put instance type first
cols = dataframe.columns.tolist()
a, b = cols.index('SKU'), cols.index('Instance Type')
cols[b], cols[a] = cols[a], cols[b]

#sorts the dataframe according to the instance type column
dataframe = dataframe[cols]
dataframe = dataframe.sort_values(by=['Instance Type'])

#Exports the dataframe as a csv file
dataframe.to_csv('out.csv', index = False)
print(dataframe.head())
