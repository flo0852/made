import pandas
import os
import urllib.request
import zipfile
from sqlalchemy import BIGINT, REAL, TEXT


def loadAndExtractFiles(directory_path: str):
    zip_file_path = os.path.join(os.getcwd(), 'gtfs.zip')
    urllib.request.urlretrieve(
        'https://gtfs.rhoenenergie-bus.de/GTFS.zip', zip_file_path)
    with zipfile.ZipFile(zip_file_path, 'r') as zObject:
        zObject.extractall(directory_path)


def getDataSet(file_path: str):
    return pandas.read_csv(file_path, usecols=[0, 2, 4, 5, 6])


def transformData(data: pandas.DataFrame):
    data.drop(data[data['zone_id']
                   != 2001].index, inplace=True)
    transformedData = dropNonGeographicData(data, 'stop_lat')
    transformedData = dropNonGeographicData(transformedData, 'stop_lon')
    return transformedData


def dropNonGeographicData(data: pandas.DataFrame, column_name: str):
    data.drop(data[data[column_name]
                   > 90].index, inplace=True)
    return data.drop(data[data[column_name]
                          < -90].index)


def createTableFromData(transformedData: pandas.DataFrame):
    dtype = {
        'stop_id': BIGINT, 'stop_name': TEXT, 'stop_lat': REAL, 'stop_lon': REAL, 'zone_id': BIGINT
    }
    transformedData.to_sql(name='stops', con='sqlite:///gtfs.sqlite',
                           if_exists='replace', index=False, dtype=dtype)


def initiatePipeline():
    directory_path = os.path.join(os.getcwd(), 'gtfs')
    loadAndExtractFiles(directory_path)
    dataSet = getDataSet(os.path.join(directory_path, 'stops.txt'))
    transformedDataSet = transformData(dataSet)
    createTableFromData(transformedDataSet)


if __name__ == '__main__':
    initiatePipeline()
