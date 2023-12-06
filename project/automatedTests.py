import os
import sqlite3
import pandas as pd
import automatedDataPipeline


def test_DataFrames():
    trainStations = automatedDataPipeline.getTrainStationsGroupedByPLZ()
    assert len(trainStations.columns) == (2)
    assert len(trainStations) > 0  # assert not empty
    assert trainStations.isnull().values.any() == False # no null values

    areaInfos = automatedDataPipeline.getAreaInfos()
    assert len(areaInfos.columns) == (3)
    assert len(areaInfos) > 0  # assert not empty
    assert areaInfos.isnull().values.any() == False # no null values

    allocation = automatedDataPipeline.getAllocation()
    assert len(allocation.columns) == (4)
    assert len(allocation) > 0  # assert not empty
    assert allocation.isnull().values.any() == False # no null values

    cars = automatedDataPipeline.getCars()
    assert len(cars.columns) == (3)
    assert len(cars) > 0  # assert not empty
    assert cars.isnull().values.any() == False # no null values


def test_system():
    automatedDataPipeline.createTablesFromCSV()
    path = "../data/final.sqlite"
    conn = sqlite3.connect(path)
    final = pd.read_sql("SELECT * FROM final", conn)
    assert os.path.isfile(path) #check if file exists
    assert len(final.columns) == (8)
    assert len(final) > 0  # assert not empty
    assert final.isnull().values.any() == False # no null values
    conn.close


if __name__ == '__main__':
    test_DataFrames()
    test_system()
