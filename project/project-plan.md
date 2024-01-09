# Project Plan

## Title
Correlation between air pollution and number of vehicles with combustion motors in Nordrhein-Westfalen.

## Main Question

1. Is the number of combustion motor vehicles the main factor in air pollution in Nordrhein-Westfalen?

## Description

<!-- Describe your data science project in max. 200 words. Consider writing about why and how you attempt it. "XY is an important problem, because... This projects analyzes XY, using method A. The results can give insights into..."-->

This analysis helps to get a clear picture of how many cars pollute the air in the state of North Rhine-Westphalia. This analysis also helps identify areas with a greater number of vehicles, which could potentially indicate a greater demand for services related to mechanical workshops and gasoline consumption. The resulting information may be relevant in a study that evaluates the number of vehicles a city can afford to have because high pollution can cause a high rate of respiratory diseases.

## Datasources

<!-- Describe each datasources you plan to use in a section. Use the prefic "DatasourceX" where X is the id of the datasource. -->

### Datasource1: Stock of motor vehicles by vehicle type in Nordrhein-Westfalen
* Metadata URL: https://mobilithek.info/offers/-4132669826481765343
* Data URL: https://www.landesdatenbank.nrw.de/ldbnrwws/downloader/00/tables/46251-02iz_00.csv
* Data Type: CSV

Stock of motor vehicles by motor vehicle type of cities:   Köln,   Münster, Detmold, Arnsberg, Düsseldorf

### Datasource2: Annual parameters of air pollutants in Nordrhein-Westfalen
* Metadata URL: https://www.opengeodata.nrw.de/produkte/umwelt_klima/luftqualitaet/luqs/eu_jahreskenngroessen/
* Data URL: https://www.opengeodata.nrw.de/produkte/umwelt_klima/luftqualitaet/luqs/eu_jahreskenngroessen/LUQS-EU-Kenngroessen-2022.xlsx
* Data Type: xlsx

Annual parameters of air pollutants in Nordrhein-Westfalen for 2022: Nitrogen dioxide, fine dust (PM10), fine dust (PM2.5), sulfur dioxide, benzene, lead, arsenic, cadmium, nickel, benzopyrene

## Work Packages

<!-- List of work packages ordered sequentially, each pointing to an issue with more details. -->

1. Explore Datasources [#1][i1]
2. Clean & Transform data [#2][i2]
3. Build an Automated Data Pipeline [#3][i3]
4. Improve Data Pipeline [#4][i4]
5. Deploy the Tests [#5][i5]
6. Create libraries requirements [#6][i6]
7. Continuous integration with Github actions [#7][i7]
8. Explore and Analyze Resulting Data 
9. Write Final Report and submit [#8][i8]
10. Submit the project #10

[i1]: https://github.com/JoaquinAyzanoa/made-template_ws2324/issues/6
[i2]: https://github.com/JoaquinAyzanoa/made-template_ws2324/issues/8
[i3]: https://github.com/JoaquinAyzanoa/made-template_ws2324/issues/11
[i4]: https://github.com/JoaquinAyzanoa/made-template_ws2324/pull/14
[i5]: https://github.com/JoaquinAyzanoa/made-template_ws2324/issues/15
[i6]: https://github.com/JoaquinAyzanoa/made-template_ws2324/issues/19
[i7]: https://github.com/JoaquinAyzanoa/made-template_ws2324/issues/17
[i8]: https://github.com/JoaquinAyzanoa/made-template_ws2324/issues/23
