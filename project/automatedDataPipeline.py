import pandas
from sqlalchemy import BIGINT, REAL, TEXT


def getTrainStationsData():
    trainStationsColNames = ['ZIP code']
    return pandas.read_csv(
        filepath_or_buffer='https://download-data.deutschebahn.com/static/datasets/stationsdaten/DBSuS-Uebersicht_Bahnhoefe-Stand2020-03.csv', sep=";", usecols=[8], names=trainStationsColNames, skiprows=[0])


def transformTrainStationsData(trainStationsData: pandas.DataFrame):
    trainStationsData.dropna(inplace=True)  # drop null values
    trainStationsGrouped = trainStationsData['ZIP code'].value_counts(
    ).reset_index(name='Number of train stations').rename(columns={"index": "ZIP code"})
    dtype_trainStationsGroupedData = {
        'Number of train stations': BIGINT
    }

    trainStationsGrouped.to_sql(name='trainStationsGrouped',
                                con='sqlite:///../data/trainStationsGrouped.sqlite', if_exists='replace', index=False, dtype=dtype_trainStationsGroupedData)
    return trainStationsGrouped


def getAreaInfosData():
    dtype_areaInfos = {
        'ZIP code': BIGINT,
        'Number of residents': BIGINT,
        'Square km': REAL
    }
    areaInfosColNames = ['ZIP code', 'Number of residents', 'Square km']
    areaInfos = pandas.read_csv(
        filepath_or_buffer='https://downloads.suche-postleitzahl.org/v2/public/plz_einwohner.csv', sep=",", usecols=[0, 2, 3], names=areaInfosColNames, skiprows=[0])
    areaInfos.to_sql(name='areaInfos', con='sqlite:///../data/areaInfos.sqlite',
                     if_exists='replace', index=False, dtype=dtype_areaInfos)
    return areaInfos


def getCarsData():
    carsColNames = ['County name', 'Number of PKWs']
    return pandas.read_csv(filepath_or_buffer='https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0020_00.csv',
                           sep=r';', encoding='latin-1', skiprows=6, usecols=[2, 7], names=carsColNames, engine='python')  # also , as separator to separate the type of the county (Landkreis or kreisfreie Stadt)


def transformCarsData(carsData):
    carsData.dropna(inplace=True)  # Drop null values
    # remove rows without data for number of PKWs
    carsData.drop(carsData[carsData['Number of PKWs']
                  == '-'].index, inplace=True)
    # extra column for type of county
    carsData['Type of county'] = 'kreisfreie Stadt'
    # extract type of county if it's 'Landkreis'
    mask2 = carsData['County name'].str.contains(r', Landkreis')
    # mark type of county in extra column
    carsData.loc[mask2, 'Type of county'] = 'Landkreis'
    # remove ', Landkreis' from the county name for join
    carsData['County name'].replace(
        ', Landkreis', '', inplace=True, regex=True)
    # remove ', kreisfreie Stadt' from the county name for join
    carsData['County name'].replace(
        ', kreisfreie Stadt', '', inplace=True, regex=True)

    dtype_carsData = {
        'County name': TEXT,
        'Type of county': TEXT,
        'Number of PKWs': BIGINT,
    }

    carsData.to_sql(name='cars', con='sqlite:///../data/cars.sqlite',
                    if_exists='replace', index=False, dtype=dtype_carsData)
    return carsData


def getAllocationData():
    allocationColNames = ['Town', 'ZIP code', 'County name']
    return pandas.read_csv(
        filepath_or_buffer='https://downloads.suche-postleitzahl.org/v2/public/zuordnung_plz_ort.csv', sep=",", usecols=[2, 3, 4], names=allocationColNames, skiprows=[0]).drop_duplicates(subset='ZIP code', keep='last')  # get dataset + remove duplicate zip codes


def transformAllocationData(allocationData):
    # if cell for 'Landkreis' is null, it's a "kreisfreie Stadt", so the name of the city can be taken as 'Landkreis' for the join
    allocationData['County name'] = allocationData['County name'].fillna(
        allocationData['Town'])
    # mark type of county in extra column
    allocationData['Type of county'] = 'kreisfreie Stadt'
    mask1 = allocationData['County name'].str.contains(
        r'Landkreis')  # extract type of county if it's 'Landkreis'
    # mark type of county in extra column
    allocationData.loc[mask1, 'Type of county'] = 'Landkreis'
    # remove 'Landkreis ' from the county name for join
    allocationData['County name'].replace(
        'Landkreis ', '', inplace=True, regex=True)
    # extract type of county if it's 'Kreis ' to 'Landkreis' in extra column
    mask2 = allocationData['County name'].str.contains(r'Kreis ')
    # mark type of county in extra column
    allocationData.loc[mask2, 'Type of county'] = 'Landkreis'
    # remove 'Kreis ' from the county name for join
    allocationData['County name'].replace(
        'Kreis ', '', inplace=True, regex=True)

    dtype_allocationData = {
        'Town': TEXT,
        'ZIP code': BIGINT,
        'County name': TEXT,
        'Type of county': TEXT,
    }
    allocationData.to_sql(name='allocation', con='sqlite:///../data/allocation.sqlite',
                          if_exists='replace', index=False, dtype=dtype_allocationData)
    return allocationData


def createTablesFromData(trainStationsData, carsData, areaInfosData, allocationData):
    trainStationsGrouped = transformTrainStationsData(trainStationsData)
    areaInfosWithTrainStations = pandas.merge(
        areaInfosData, trainStationsGrouped, how='left', left_on='ZIP code', right_on='ZIP code')

    cars = transformCarsData(carsData)

    allocation = transformAllocationData(allocationData)
    areaInfosWithTrainStationsWithKreis = pandas.merge(
        areaInfosWithTrainStations, allocation, left_on='ZIP code', right_on='ZIP code')

    areaInfosWithTrainStationsWithKreisGrouped = areaInfosWithTrainStationsWithKreis.groupby(
        ['County name', 'Type of county']).sum().reset_index().drop('ZIP code', axis=1)

    dType_final = {
        'County name': TEXT,
        'Type of county': TEXT,
        'Number of residents': BIGINT,
        'Square km': REAL,
        'Number of train stations': BIGINT,
        'Number of PKWs': BIGINT,
        'Number of PKWs per 1000 residents': REAL,
        'Train Stations per qkm': REAL,
    }
    final = pandas.merge(areaInfosWithTrainStationsWithKreisGrouped,
                         cars, left_on=['County name', 'Type of county'], right_on=['County name', 'Type of county'])

    final['Train Stations per qkm'] = final['Number of train stations'] / \
        final['Square km']
    final['Number of PKWs per 1000 residents'] = final['Number of PKWs'].astype(
        'int32') / final['Number of residents'] * 1000
    final.to_sql(name='final', con='sqlite:///../data/final.sqlite',
                 if_exists='replace', index=False, dtype=dType_final)
    return final


def initiatePipeline():
    trainStationsData = getTrainStationsData()
    carsData = getCarsData()
    areaInfosData = getAreaInfosData()
    allocationData = getAllocationData()
    createTablesFromData(trainStationsData, carsData,
                         areaInfosData, allocationData)

if __name__ == '__main__':
    initiatePipeline()
