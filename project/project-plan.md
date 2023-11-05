# Project Plan

## Title
Correlation of train stations and cars

## Main Question
How does the number of train stations per qm in a municipality affect the number of cars per 1000 residents in this municipality?

## Description
In days of global climate change, it gets more and more important to go climate friendly. One possibility to do this is by using the train instead of the car. Or in the best case not even having a car.
So the aim of this project is to find out, whether the number of train stations per qkm has an influence on the number of cars per 1000 residents in this municipality.
So if the answer to this question would be true, it could be a possibility to build more train stations in order to decrease the number of cars and so become more climate friendly.

## Datasources

### Datasource1: Train stations per zip code
* Metadata URL: https://data.deutschebahn.com/dataset/data-stationsdaten/resource/dfddd39b-b74c-40b7-9b60-d265ee2473cb.html
* Data URL: https://download-data.deutschebahn.com/static/datasets/stationsdaten/DBSuS-Uebersicht_Bahnhoefe-Stand2020-03.csv
* Data Type: CSV

Dataset which lists the existing train stations in Germany with the corresponding zip code.

### Datasource2: Residents and area per zip code
* Metadata URL: https://www.suche-postleitzahl.org/downloads
* Data URL: https://downloads.suche-postleitzahl.org/v2/public/plz_einwohner.csv
* Data Type: CSV

Dataset which contains the number of residents and the area in qm per zip code.
### Datasource3: Number of cars per municipality
* Metadata URL: https://mobilithek.info/offers/-7245790047701635178
* Data URL: https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0020_00.csv
* Data Type: CSV

Dataset which contains the number of cars per municipality.

### Datasource4: Allocation table for zip code and municipality 
* Metadata URL: https://www.suche-postleitzahl.org/downloads
* Data URL: https://downloads.suche-postleitzahl.org/v2/public/zuordnung_plz_ort.csv
* Data Type: CSV

Dataset which shows, which zip code belongs to which municipality.
This dataset is needed, because the number of cars is listed by municipality, but the train stations, number of residents and area ist listed by zip code. 

## Work Packages


1. Explore Datasets [#1][i1]
2. Write script to pull and clean datasets [#2][i2]
3. Analyze resulting data [#3][i3]
4. Add automated tests [#4][i4]
5. Continious Integration [#5][i5]
6. Write report [#6][i6]

[i1]: https://github.com/flo0852/made/issues/1
[i2]: https://github.com/flo0852/made/issues/2
[i3]: https://github.com/flo0852/made/issues/3
[i4]: https://github.com/flo0852/made/issues/4
[i5]: https://github.com/flo0852/made/issues/5
[i6]: https://github.com/flo0852/made/issues/6
