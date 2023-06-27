# Landalytics (Land-analytics)

Landalytics is a project to explore the UK's open geospatial datasets for use in the property industry.

Datasets of interest:
* [Land Registry Price Paid Data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads)
* [UK House Price Index Data](https://www.gov.uk/government/statistical-data-sets/uk-house-price-index-data-downloads-march-2023)
* [Land Registry Transaction Data](https://use-land-property-data.service.gov.uk/datasets/td/download)
* [Energy Performance Certificates Data (EPC)](https://epc.opendatacommunities.org/)
* [OS Open UPRN](https://beta.ordnancesurvey.co.uk/products/os-open-uprn)
* [Codepoint Open](https://beta.ordnancesurvey.co.uk/products/code-point-open)
* Open Street Map - [Wiki](https://wiki.openstreetmap.org/wiki/Downloading_data) & [Geofabrik](http://download.geofabrik.de/europe/great-britain.html)
* [ONS UPRN Directory](https://geoportal.statistics.gov.uk/datasets/ons-uprn-directory-april-2023/about)
* And more...

Since Addressbase is a premium product, linking addresses to a specific coordinate (point) is not easily doable. 

For now, we can aggregate to postcode, sector, district and area levels.


## Prerequisites
`Libpostal` is required to run the address parsing, check instructions at: https://github.com/openvenues/libpostal

Ensure you have the libpostal libraries on the `libpostal` folder in the project root, the spark jobs that need libpostal will need to reference the libs and some tests require the path to the libs too.

## Subprojects

### data-source-land-registry
Contains the Model and ETLs for the land registry price paid data

### data-source-epc
Contains the Model and ETLs for the EPC data

### data-source-codepoint-open
Contains the Model and ETLs for the codepoint open data

### data-source-os-open-uprn
Contains the Model and ETLs for the os open uprn data

### landalytics-address-core
Contains the Model and ETLs for constructing the landalytics address core. 
This will contain coords and addresses used for address matching on other data sources

### landalytics-address-matcher
The spark app here allows address-matching of datasets that contains parsedAddress field and performs address-matching on the landalytics-address-core dataset


### utilities
Contains utility functions for use across other subprojects

#### address-parsers
Contains the ParserAddress model and apply methods required to parse addresses. Utilizes libpostal to do parse address.

#### etl-helpers
Contains common etl methods which can be used across any etl subproject

#### spark-helpers
Contains spark helpers traits and abstract classes to align spark jobs to initialize spark as well as using io.circe to allow passing in of custom json configuration for spark jobs.

## EPC Data contains addresses and UPRNs
EPC data contains the UPRN for the EPC certificate, we can match addresses with EPC addresses to get the coords.

[Some datasource with addresses] -> [EPC Addresses] -> [OS Open UPRN for coords]

We can explore this route

## Running Tests

Example gradle command to run tests:

```shell
./gradlew :utilities:address-parsers:test --tests "com.landalytics.utilities.addressparsers.ParsedAddessModelTest" --rerun-tasks
```