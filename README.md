# Spark
SENG-5709 Spark/Databricks project code and data

## Datasets

### CDC with geography
https://data.cdc.gov/Case-Surveillance/COVID-19-Case-Surveillance-Public-Use-Data-with-Ge/n8mc-b4w4

Downloaded file had 69,664,983 rows total.  Extracted just rows for MN using grep (1,427,734 rows).  The resulting file was too large for GitHub, so split it into segments by 6 month period.

`COVID-MN-2020H1.csv`
`COVID-MN-2020H2.csv`
`COVID-MN-2021H1.csv`
`COVID-MN-2021H2.csv`
`COVID-MN-2022H1.csv`

### Census Bureau 2020 median income by MN county (S1903)

`ACSST5Y2020.S1903-2022-04-08T200146.csv`

### Census Bureau 2020 educational attainment by MN county (S1501)

`ACSST5Y2020.S1501-2022-04-08T200457.csv`

### MN Department of Health, County profiles
https://data.web.health.state.mn.us/web/mndata/download-county-data

`MN_county_profile.csv`

### MN Department of Health
https://mdhprovidercontent.web.health.state.mn.us/showprovideroutput.cfm
download of xls, saved as csv

`provider_list.csv`

### Minnesota Urban/Rural Divide
https://mn.gov/admin/demography/reports-resources/greater-mn-refined-and-revisited.jsp

County classification: Table 6, Greater MN Refined and Revisited
  - Entirely rural
  - Town/rural mix
  - Urban/town/rural mix (exclude these?)
  - Entirely urban

`county.csv`
