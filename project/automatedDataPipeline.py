import pandas
from sqlalchemy import BIGINT, REAL, TEXT

# TODO: eliminate NULL rows
# TODO: dTypes


def getTrainStationsGrouped():
    dtype_trainStations = {
        'PLZ': BIGINT
    }
    trainStations = pandas.read_csv(
        filepath_or_buffer='https://download-data.deutschebahn.com/static/datasets/stationsdaten/DBSuS-Uebersicht_Bahnhoefe-Stand2020-03.csv', sep=";", usecols=[8])
    trainStations.dropna(inplace=True)
    trainStations.to_sql(
        name='trainStations', con='sqlite:///trainStations.sqlite', if_exists='replace', index=False, dtype=dtype_trainStations)
    trainStationsGrouped = trainStations['PLZ'].value_counts(
    ).reset_index(name='Number of train Stations')
    trainStationsGrouped.to_sql(name='trainStationsGrouped',
                                con='sqlite:///trainStationsGrouped.sqlite', if_exists='replace', index=False)
    return trainStationsGrouped


def getAreaInfos():
    dtype_areaInfos = {
        'plz': BIGINT,
        'einwohner': BIGINT,
        'qkm': REAL
    }
    areaInfos = pandas.read_csv(
        filepath_or_buffer='https://downloads.suche-postleitzahl.org/v2/public/plz_einwohner.csv', sep=",", usecols=[0, 2, 3])
    areaInfos.to_sql(name='areaInfos', con='sqlite:///areaInfos.sqlite',
                     if_exists='replace', index=False, dtype=dtype_areaInfos)
    return areaInfos


def getCars():
    dtype_cars = {
        'Kreis': TEXT,
        'Kreis Art': TEXT,
        'Anzahl PKW': BIGINT,
    }
    carsColNames = ['Kreis', 'Kreis Art', 'Anzahl PKW']
    cars = pandas.read_csv(filepath_or_buffer='https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0020_00.csv',
                           sep=r',|;', encoding='latin-1', skiprows=6, usecols=[2, 3, 4], names=carsColNames, engine='python')
    cars.dropna(inplace=True)
    cars['Kreis Art'].replace(' kreisfreie Stadt', 'kreisfreie Stadt',
                              inplace=True, regex=True)
    cars['Kreis Art'].replace(' Landkreis', 'Landkreis',
                              inplace=True, regex=True)
    cars.to_sql(name='cars', con='sqlite:///cars.sqlite',
                if_exists='replace', index=False, dtype=dtype_cars)
    return cars


def getAllocation():
    allocation = pandas.read_csv(
        filepath_or_buffer='https://downloads.suche-postleitzahl.org/v2/public/zuordnung_plz_ort.csv', sep=",", usecols=[2, 3, 4]).drop_duplicates(subset='plz', keep='last')
    allocation['landkreis'] = allocation['landkreis'].fillna(allocation['ort'])
    allocation['Kreis Art'] = 'kreisfreie Stadt'
    mask = allocation['landkreis'].str.contains(r'Landkreis')
    allocation.loc[mask, 'Kreis Art'] = 'Landkreis'
    allocation['landkreis'].replace('Landkreis ', '', inplace=True, regex=True)
    allocation.to_sql(name='allocation', con='sqlite:///allocation.sqlite',
                      if_exists='replace', index=False)
    return allocation


def createTablesFromCSV():
    trainStationsGrouped = getTrainStationsGrouped()
    areaInfos = getAreaInfos()

    areaInfosWithTrainStations = pandas.merge(
        areaInfos, trainStationsGrouped, left_on='plz', right_on='index').drop('index', axis=1)
    areaInfosWithTrainStations.to_sql(name='areaInfosWithTrainStations', con='sqlite:///areaInfosWithTrainStations.sqlite',
                                      if_exists='replace', index=False)

    cars = getCars()

    allocation = getAllocation()

    areaInfosWithTrainStationsWithKreis = pandas.merge(
        areaInfosWithTrainStations, allocation, left_on='plz', right_on='plz')
    areaInfosWithTrainStationsWithKreis.to_sql(name='areaInfosWithTrainStationsWithKreis', con='sqlite:///areaInfosWithTrainStationsWithKreis.sqlite',
                                               if_exists='replace', index=False)
    dType_areaInfosWithTrainStationsWithKreisGrouped = {
        'landkreis': TEXT,
        'Kreis Art': TEXT,
        'einwohner': BIGINT,
        'qkm': REAL,
        'Number of train stations': BIGINT,
        'einwohner': BIGINT,
    }
    areaInfosWithTrainStationsWithKreisGrouped = areaInfosWithTrainStationsWithKreis.groupby(
        ['landkreis', 'Kreis Art']).sum().reset_index().drop('plz', axis=1)
    areaInfosWithTrainStationsWithKreisGrouped.to_sql(name='areaInfosWithTrainStationsWithKreisGrouped',
                                                      con='sqlite:///areaInfosWithTrainStationsWithKreisGrouped.sqlite', if_exists='replace', index=False, dtype=dType_areaInfosWithTrainStationsWithKreisGrouped)

    final = pandas.merge(areaInfosWithTrainStationsWithKreisGrouped,
                         cars, how='left', left_on=['landkreis', 'Kreis Art'], right_on=['Kreis', 'Kreis Art'])
    final.to_sql(name='final', con='sqlite:///final.sqlite',
                 if_exists='replace', index=False)


if __name__ == '__main__':
    createTablesFromCSV()
