import sqlite3
import pandas as pd
import automatedDataPipeline


def test_DataFrames():
    trainStations = automatedDataPipeline.getTrainStationsGroupedByPLZ()
    assert len(trainStations.columns) == (2)
    assert len(trainStations) > 0  # assert not empty

    areaInfos = automatedDataPipeline.getAreaInfos()
    assert len(areaInfos.columns) == (3)
    assert len(areaInfos) > 0  # assert not empty

    allocation = automatedDataPipeline.getAllocation()
    assert len(allocation.columns) == (4)
    assert len(allocation) > 0  # assert not empty

    cars = automatedDataPipeline.getCars()
    assert len(cars.columns) == (3)
    assert len(cars) > 0  # assert not empty


def test_system():
    automatedDataPipeline.createTablesFromCSV()
    conn = sqlite3.connect("../data/final.sqlite")
    final = pd.read_sql("SELECT * FROM final", conn)
    assert len(final.columns) == (8)
    assert len(final) > 0  # assert not empty
    conn.close


if __name__ == '__main__':
    test_DataFrames()
    test_system()
