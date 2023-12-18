import os
import sqlite3
import pandas as pd
import automatedDataPipeline
trainStationsMockData = pd.DataFrame([
    [91126],
    [91126],
    [91126],
    [None],
    [25541],
    [25541],
    [25719],
    [90453],
    [None],
], columns=['ZIP code'])

carsMockData = pd.DataFrame([
    ['Schwabach, kreisfreie Stadt', 10000],
    ['Nürnberg, kreisfreie Stadt', 10000],
    ['Dithmarschen, Landkreis', 20000],
    ['Bergstraße, Landkreis', 20000],
    ['Stadt', None],
    [None, 10],
], columns=['County name', 'Number of PKWs'])

areaInfosMockData = pd.DataFrame([
    [91126, 10000, 5],
    [90453, 20000, 10],
    [90451, 20000, 10],
    [25541, 2000, 2],
    [25719, 2000, 2],
    [94673, 4000, 4],
], columns=['ZIP code', 'Number of residents', 'Square km'])

allocationMockData = pd.DataFrame([
    ['Brunsbüttel', 25541, 'Landkreis Dithmarschen'],
    ['Busenwurth', 25719, 'Landkreis Dithmarschen'],
    ['Schwabach', 91126, None],
    ['Nürnberg', 90451, None],
    ['Nürnberg', 90453, None],
    ['Zwingenberg', 94673, 'Kreis Bergstraße'],
], columns=['Town', 'ZIP code', 'County name'])


def test_system():
    automatedDataPipeline.createTablesFromCSV()
    path = "../data/final.sqlite"
    conn = sqlite3.connect(path)
    final = pd.read_sql("SELECT * FROM final", conn)
    assert os.path.isfile(path)  # check if file exists
    assert len(final.columns) == (8)
    assert len(final) > 0  # assert not empty
    assert final.isnull().values.any() == False  # no null values
    assert "Schwabach" in final['County name'].values
    rowDataFrame = final.loc[final["County name"] == 'Schwabach']
    assert len(rowDataFrame) == 1
    row = rowDataFrame.iloc[0]
    assert row['Type of county'] == "kreisfreie Stadt"
    conn.close


def test_transformTrainStationsData():
    expected = pd.DataFrame([
        [91126.0, 3],
        [25541.0, 2],
        [25719.0, 1],
        [90453.0, 1],
    ], columns=['ZIP code', 'Number of train stations'])
    trainStations = automatedDataPipeline.transformTrainStationsData(
        trainStationsMockData.copy())
    assert len(trainStations.columns) == 2  # number of columns
    assert len(trainStations) == 4  # number of rows
    assert trainStations.isnull().values.any() == False  # no null values
    assert trainStations.equals(expected)


def test_transformCarsData():
    expected = pd.DataFrame([
        ['Schwabach', 10000.0, 'kreisfreie Stadt'],
        ['Nürnberg', 10000.0, 'kreisfreie Stadt'],
        ['Dithmarschen', 20000.0, 'Landkreis'],
        ['Bergstraße', 20000.0, 'Landkreis'],
    ], columns=['County name', 'Number of PKWs', 'Type of county'])
    cars = automatedDataPipeline.transformCarsData(
        carsMockData.copy())
    assert len(cars.columns) == (3)
    assert len(cars) > 0  # assert not empty
    assert cars.isnull().values.any() == False  # no null values
    assert cars.equals(expected)


def test_transformAllocationData():
    expected = pd.DataFrame([
        ['Brunsbüttel', 25541, 'Dithmarschen', 'Landkreis'],
        ['Busenwurth', 25719, 'Dithmarschen', 'Landkreis'],
        ['Schwabach', 91126, 'Schwabach', 'kreisfreie Stadt'],
        ['Nürnberg', 90451, 'Nürnberg', 'kreisfreie Stadt'],
        ['Nürnberg', 90453, 'Nürnberg', 'kreisfreie Stadt'],
        ['Zwingenberg', 94673, 'Bergstraße', 'Landkreis'],
    ], columns=['Town', 'ZIP code', 'County name', 'Type of county'])

    allocation = automatedDataPipeline.transformAllocationData(
        allocationMockData.copy())
    assert len(allocation.columns) == (4)
    assert len(allocation) > 0  # assert not empty
    assert allocation.isnull().values.any() == False  # no null values
    assert allocation.equals(expected)


def test_systemMock():
    final = automatedDataPipeline.createTablesFromData(
        trainStationsMockData.copy(), carsMockData.copy(), areaInfosMockData.copy(), allocationMockData.copy())

    expected = pd.DataFrame([
        ['Bergstraße', 'Landkreis', 4000, 4, 0.0, 20000.0, 0.00, 5000.0],
        ['Dithmarschen', 'Landkreis', 4000, 4, 3.0, 20000.0, 0.75, 5000.0],
        ['Nürnberg', 'kreisfreie Stadt', 40000, 20, 1.0, 10000.0, 0.05, 250],
        ['Schwabach', 'kreisfreie Stadt', 10000, 5, 3.0, 10000.0, 0.60, 1000],
    ], columns=['County name', 'Type of county', 'Number of residents', 'Square km', 'Number of train stations', 'Number of PKWs', 'Train Stations per qkm', 'Number of PKWs per 1000 residents'])
    assert expected.equals(final)


if __name__ == '__main__':
    test_transformTrainStationsData()
    test_transformCarsData()
    test_transformAllocationData()
    test_systemMock()
