import pandas
from sqlalchemy import BIGINT, REAL, TEXT

def getTrainStationsGroupedByPLZ():
    dtype_trainStations = {
        'ZIP code': BIGINT
    }
    trainStationsColNames = ['ZIP code']
    trainStations = pandas.read_csv(
        filepath_or_buffer='https://download-data.deutschebahn.com/static/datasets/stationsdaten/DBSuS-Uebersicht_Bahnhoefe-Stand2020-03.csv', sep=";", usecols=[8], names=trainStationsColNames, skiprows=[0])
    
    trainStations.dropna(inplace=True) #drop null values
    trainStationsGrouped = trainStations['ZIP code'].value_counts(
    ).reset_index(name='Number of train stations')
    trainStationsGrouped.to_sql(name='trainStationsGrouped',
                                con='sqlite:///../data/trainStationsGrouped.sqlite', if_exists='replace', index=False, dtype=dtype_trainStations)
    return trainStationsGrouped


def getAreaInfos():
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


def getCars():
    dtype_cars = {
        'County name': TEXT,
        'Type of county': TEXT,
        'Number of PKWs': BIGINT,
    }
    carsColNames = ['County name', 'Number of PKWs']
    cars = pandas.read_csv(filepath_or_buffer='https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0020_00.csv',
                           sep=r';', encoding='latin-1', skiprows=6, usecols=[2, 7], names=carsColNames, engine='python') # also , as separator to separate the type of the county (Landkreis or kreisfreie Stadt)
    cars.dropna(inplace=True) # Drop null values
    cars.drop(cars[cars['Number of PKWs'] == '-'].index, inplace=True) # remove rows without data for number of PKWs
    cars['Type of county'] = 'kreisfreie Stadt' # extra column for type of county 
    mask2 = cars['County name'].str.contains(r', Landkreis') # extract type of county if it's 'Landkreis'
    cars.loc[mask2, 'Type of county'] = 'Landkreis' # mark type of county in extra column
    cars['County name'].replace(', Landkreis', '', inplace=True, regex=True) # remove ', Landkreis' from the county name for join
    cars['County name'].replace(', kreisfreie Stadt', '', inplace=True, regex=True) # remove ', kreisfreie Stadt' from the county name for join
    
    cars.to_sql(name='cars', con='sqlite:///../data/cars.sqlite',
                if_exists='replace', index=False, dtype=dtype_cars)
    return cars


def getAllocation():
    dtype_allocation = {
        'Town': TEXT,
        'ZIP code': BIGINT,
        'County name': TEXT,
        'Type of county': TEXT,
    }
    allocationColNames = ['Town', 'ZIP code', 'County name']
    
    allocation = pandas.read_csv(
        filepath_or_buffer='https://downloads.suche-postleitzahl.org/v2/public/zuordnung_plz_ort.csv', sep=",", usecols=[2, 3, 4], names=allocationColNames, skiprows=[0]).drop_duplicates(subset='ZIP code', keep='last') #get dataset + remove duplicate zip codes
    
    allocation['County name'] = allocation['County name'].fillna(allocation['Town']) # if cell for 'Landkreis' is null, it's a "kreisfreie Stadt", so the name of the city can be taken as 'Landkreis' for the join
    allocation['Type of county'] = 'kreisfreie Stadt' # mark type of county in extra column
    mask1 = allocation['County name'].str.contains(r'Landkreis') # extract type of county if it's 'Landkreis'
    allocation.loc[mask1, 'Type of county'] = 'Landkreis' # mark type of county in extra column
    allocation['County name'].replace('Landkreis ', '', inplace=True, regex=True) # remove 'Landkreis ' from the county name for join
    mask2 = allocation['County name'].str.contains(r'Kreis ') # extract type of county if it's 'Kreis ' to 'Landkreis' in extra column
    allocation.loc[mask2, 'Type of county'] = 'Landkreis' # mark type of county in extra column
    allocation['County name'].replace('Kreis ', '', inplace=True, regex=True) # remove 'Kreis ' from the county name for join

    allocation.to_sql(name='allocation', con='sqlite:///../data/allocation.sqlite',
                      if_exists='replace', index=False, dtype=dtype_allocation)
    return allocation


def createTablesFromCSV():
    trainStationsGrouped = getTrainStationsGroupedByPLZ()
    areaInfos = getAreaInfos()

    areaInfosWithTrainStations = pandas.merge(
        areaInfos, trainStationsGrouped, left_on='ZIP code', right_on='index').drop('index', axis=1)

    cars = getCars()

    allocation = getAllocation()

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
    }
    final = pandas.merge(areaInfosWithTrainStationsWithKreisGrouped,
                         cars, left_on=['County name', 'Type of county'], right_on=['County name', 'Type of county'])
    final['Train Stations per qkm'] = final['Number of train stations'] / final['Square km']
    final['Number of PKWs per 1000 residents'] = final['Number of PKWs'].astype('int32') / final['Number of residents'] *1000
    final.to_sql(name='final', con='sqlite:///../data/final.sqlite',
                 if_exists='replace', index=False, dtype=dType_final)


if __name__ == '__main__':
    createTablesFromCSV()
