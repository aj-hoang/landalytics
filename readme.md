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

## EPC Data contains addresses and UPRNs
EPC data contains the UPRN for the EPC certificate, we can match addresses with EPC addresses to get the coords.

[Some datasource with addresses] -> [EPC Addresses] -> [OS Open UPRN for coords]

We can explore this route

## Running Tests

Example gradle command to run tests:

```shell
./gradlew :utilities:address-parsers:test --tests "com.landalytics.utilities.addressparsers.ParsedAddessModelTest" --rerun-tasks
```