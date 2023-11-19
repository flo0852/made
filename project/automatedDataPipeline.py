import pandas
from sqlalchemy import BIGINT, REAL, TEXT

# TODO: eliminate NULL rows
def createTablesFromCSV():
    dtype_trainStations = {
        'PLZ': BIGINT
    }
    trainStations = pandas.read_csv(
        filepath_or_buffer='https://download-data.deutschebahn.com/static/datasets/stationsdaten/DBSuS-Uebersicht_Bahnhoefe-Stand2020-03.csv', sep=";", usecols=[8])
    trainStations.dropna(inplace=True)
    trainStations.to_sql(
        name='trainStations', con='sqlite:///trainStations.sqlite', if_exists='replace', index=False, dtype=dtype_trainStations)

    dtype_areaInfos = {
        'plz': BIGINT,
        'einwohner': BIGINT,
        'qkm': REAL
    }
    areaInfos = pandas.read_csv(
        filepath_or_buffer='https://downloads.suche-postleitzahl.org/v2/public/plz_einwohner.csv', sep=",", usecols=[0, 2, 3])
    areaInfos.to_sql(name='areaInfos', con='sqlite:///areaInfos.sqlite',
                     if_exists='replace', index=False, dtype=dtype_areaInfos)

    dtype_cars = {
        'kreis': TEXT,
        'Anzahl PKW': BIGINT,
    }
    carsColNames = ['Kreis', 'Anzahl PKW']
    cars = pandas.read_csv(filepath_or_buffer='https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0020_00.csv',
                           sep=r',|;', encoding='latin-1', skiprows=6, usecols=[2, 4], names=carsColNames, engine='python')
    cars.dropna(inplace=True)
    cars.to_sql(name='cars', con='sqlite:///cars.sqlite',
                if_exists='replace', index=False, dtype=dtype_cars)

    dtype_cars = {
        'plz': BIGINT,
        'landkreis': TEXT,
    }
    allocation = pandas.read_csv(
        filepath_or_buffer='https://downloads.suche-postleitzahl.org/v2/public/zuordnung_plz_ort.csv', sep=",", usecols=[3, 4])
    allocation.to_sql(name='allocation', con='sqlite:///allocation.sqlite',
                      if_exists='replace', index=False, dtype=dtype_cars)


if __name__ == '__main__':
    createTablesFromCSV()
