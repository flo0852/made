import pandas
from sqlalchemy import BIGINT, REAL, TEXT
dataset = pandas.read_csv(filepath_or_buffer = 'https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv', sep = ";")
dtype = {
    'column_1' : BIGINT, 'column_2' : TEXT, 'column_3' : TEXT, 'column_4' : TEXT, 'column_5' : TEXT, 'column_6' : TEXT, 'column_7' : REAL, 'column_8' : REAL, 'column_9' : BIGINT, 'column_10' : REAL, 'column_11' : TEXT, 'column_12' : TEXT, 'geo_punkt' : TEXT,
}
dataset.to_sql(name = 'airports', con = 'sqlite:///airports.sqlite', if_exists = 'replace', index = False, dtype = dtype)