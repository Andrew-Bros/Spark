# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Project: COVID-19 - The Urban/Rural Divide
# MAGIC Andrew Bros
# MAGIC 
# MAGIC SENG-5709
# MAGIC 
# MAGIC ## Imports and constants

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, trim, upper, when

S3BUCKET = "s3://seng-5709-spark"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define schema and load datasets
# MAGIC 
# MAGIC ### CDC case data with geography
# MAGIC https://data.cdc.gov/Case-Surveillance/COVID-19-Case-Surveillance-Public-Use-Data-with-Ge/n8mc-b4w4
# MAGIC 
# MAGIC   - Using unix tools, extracted the header and rows for res_state = MN to reduce the dataset from 69 to 1.4 million rows.
# MAGIC   - Use Spark to limit data to April 2020 through February 2022
# MAGIC   - Normalize hosp_yn and death_yn fields

# COMMAND ----------

case_schema = StructType([
    StructField("case_month", DateType(), False),
    StructField("res_state", StringType(), False),
    StructField("state_fips_code", IntegerType(), True),
    StructField("res_county", StringType(), False),
    StructField("county_fips_code", IntegerType(), True),
    StructField("age_group", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("race", StringType(), True),
    StructField("ethnicity", StringType(), True),
    StructField("case_positive_specimen_interval", IntegerType(), True),
    StructField("case_onset_interval", IntegerType(), True),
    StructField("process", StringType(), True),
    StructField("exposure_yn", StringType(), True),
    StructField("current_status", StringType(), True),
    StructField("symptom_status", StringType(), True),
    StructField("hosp_yn", StringType(), True),
    StructField("icu_yn", StringType(), True),
    StructField("death_yn", StringType(), True),
    StructField("underlying_conditions_yn", StringType(), True),
])
raw_case_df = (spark.read.format("csv")
               .options(header="true", dateFormat="YYYY-mm")
               .schema(case_schema)
               .load(f"{S3BUCKET}/COVID-MN.csv"))
case_df = (raw_case_df
           .where("case_month between '2020-03-31' and '2022-02-28'")
           .withColumn("hosp_yn", when(upper(col("hosp_yn")) == "YES", "YES")
                       .when(upper(col("hosp_yn")) == "NO", "NO")
                       .otherwise("NA"))
           .withColumn("death_yn", when(upper(col("death_yn")) == "YES", "YES")
                       .when(upper(col("death_yn")) == "NO", "NO")
                       .otherwise("NA")))
case_df.show(5, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### County population
# MAGIC https://data.census.gov/cedsci/table?t=Population%20Total&g=0400000US27%240500000&tid=ACSDT5Y2020.B01003&tp=true
# MAGIC 
# MAGIC   - Extracted as Excel spreadsheet then saved as csv with just county, population

# COMMAND ----------

population_schema = StructType([
    StructField("county", StringType(), False),
    StructField("population", IntegerType(), False),
    ])
population_df = (spark.read.format("csv")
                 .options(header="true")
                 .schema(population_schema)
                 .load(f"{S3BUCKET}/county_population.csv"))
population_df.show(5, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### County urban/rural classification
# MAGIC https://mn.gov/admin/demography/reports-resources/greater-mn-refined-and-revisited.jsp
# MAGIC 
# MAGIC   - Extracted classification of county from Table 6, assigning county type to be one of four types:
# MAGIC 
# MAGIC   1. Urban - entirely urban
# MAGIC   2. Mix - urban/town/rural mix
# MAGIC   3. Town - town/rural mix
# MAGIC   4. Rural - entirely rural

# COMMAND ----------

county_type_schema = StructType([
    StructField("county", StringType(), False),
    StructField("type", StringType(), False),
    ])
county_type_df = (spark.read.format("csv")
                  .options(header="true")
                  .schema(county_type_schema)
                  .load(f"{S3BUCKET}/county.csv"))
county_type_df = county_type_df.withColumn("type", trim(col("type")))
display(county_type_df.groupBy("type").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### MN Department of Health, County profiles
# MAGIC https://data.web.health.state.mn.us/web/mndata/download-county-data
# MAGIC 
# MAGIC   - took a while to figure out that this file is utf-16 encoded
# MAGIC   - symptom was that State Value and Value could not be converted to double
# MAGIC   - extract median income and uninsured rate

# COMMAND ----------

profile_schema = StructType([
    StructField("county", StringType(), False),
    StructField("indicator_category", StringType(), False),
    StructField("indicator", StringType(), False),
    StructField("year", StringType(), True),
    StructField("state_value", DoubleType(), False),
    StructField("value", DoubleType(), False),
    StructField("unit_of_measure", StringType(), False),
    ])
raw_profile_df = (spark.read.format("csv")
              .options(encoding="utf-16", header="true", sep="\t")
              .schema(profile_schema)
              .load(f"{S3BUCKET}/MN_county_profile.csv"))
profile_df = (raw_profile_df.withColumn("county_uc", upper(col("county")))
              .select("county_uc", "indicator", "value"))
median_income_df = (profile_df.filter(col("indicator") == "Median household income")
                    .withColumnRenamed("value", "median_income")
                    .drop("indicator"))
uninsured_df = (profile_df.filter(col("indicator") == "Adults without health insurance")
                .withColumnRenamed("value", "uninsured")
                .drop("indicator"))
county_financial_df = median_income_df.join(uninsured_df, "county_uc")
county_financial_df.show(5, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MN Department of Health provider list
# MAGIC https://mdhprovidercontent.web.health.state.mn.us/showprovideroutput.cfm
# MAGIC 
# MAGIC   - download of xls, saved as csv
# MAGIC   - normalize two county names

# COMMAND ----------

provider_schema = StructType([
    StructField("HFID", IntegerType(), False),
    StructField("NAME", StringType(), False),
    StructField("ADDRESS", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("STATE", StringType(), True),
    StructField("ZIP", StringType(), True),
    StructField("COUNTY_CODE", IntegerType(), True),
    StructField("COUNTY_NAME", StringType(), False),
    StructField("TELEPHONE", StringType(), True),
    StructField("FAX", StringType(), True),
    StructField("ADMINISTRATOR/AUTHORIZED AGENT", StringType(), True),
    StructField("LIC_TYPE", StringType(), True),
    StructField("HOSP_BEDS", IntegerType(), False),
    StructField("BASS_BEDS", IntegerType(), True),
    StructField("PSY_HOSP_BEDS", IntegerType(), True),
    StructField("NH_BEDS", IntegerType(), True),
    StructField("BCH_BEDS", IntegerType(), True),
    StructField("SLFA_BEDS", IntegerType(), True),
    StructField("SLFB_BEDS", IntegerType(), True),
    StructField("OTHER_BEDS", IntegerType(), True),
    StructField("HCP_TYPE", StringType(), True),
    StructField("HWS", StringType(), True),
    StructField("HWS_TYPE", StringType(), True),
    StructField("OPS", StringType(), True),
    StructField("HOSP18_BEDS", IntegerType(), True),
    StructField("CAH", StringType(), True),
    StructField("DEEMED", StringType(), True),
    StructField("SWING", StringType(), True),
    StructField("PSY18_BEDS", IntegerType(), True),
    StructField("SNF_BEDS", IntegerType(), True),
    StructField("SNFNF_BEDS", IntegerType(), True),
    StructField("NF1_BEDS", IntegerType(), True),
    StructField("NF2_BEDS", IntegerType(), True),
    StructField("ICFMR_BEDS", IntegerType(), True),
    StructField("HHA", StringType(), True),
    StructField("HOSPICE", StringType(), True),
    StructField("CMHC", StringType(), True),
    StructField("CORF", StringType(), True),
    StructField("ESRD", StringType(), True),
    StructField("ASC", StringType(), True),
    StructField("PPSP", StringType(), True),
    StructField("PPSR", StringType(), True),
    StructField("REHAB", StringType(), True),
    StructField("RHC", StringType(), True),
    StructField("XRAY", StringType(), True),
    StructField("BLSS", StringType(), True),
    StructField("MOBHES", StringType(), True),
    StructField("BC", StringType(), True),
    StructField("ICFMR", StringType(), True),
    StructField("HCBS_PRV", StringType(), True),
    StructField("SE_PRV", StringType(), True),
    StructField("SNSA_PRV", StringType(), True),
    StructField("ALL_PROV", StringType(), True),
    StructField("ALL_CAPACITY", IntegerType(), True),
])
raw_provider_df = (spark.read.format("csv")
               .options(header="true")
               .schema(provider_schema)
               .load(f"{S3BUCKET}/provider_list.csv"))
provider_df = (raw_provider_df
               .withColumn("COUNTY_NAME", when(col("COUNTY_NAME") == "LESUEUR", "LE SUEUR")
                           .when(col("COUNTY_NAME") == "SAINT LOUIS", "ST. LOUIS")
                           .otherwise(col("COUNTY_NAME"))))
hospital_beds_df = (provider_df.groupBy("COUNTY_NAME").sum("HOSP_BEDS")
                    .withColumnRenamed("COUNTY_NAME", "county_uc")
                    .withColumnRenamed("sum(HOSP_BEDS)", "hospital_beds"))
hospital_beds_df.show(5, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations
# MAGIC 
# MAGIC ### Join county information into one DataFrame
# MAGIC   - join county type and population and add uppercase county name to make joins easier
# MAGIC   - join result with hospital beds to add that count for each county
# MAGIC   - join with financial information for median income and uninsured rate

# COMMAND ----------

tmp_df = (county_type_df.join(population_df, "county")
          .withColumn("county_uc", upper(col("county"))))
tmp2_df = (tmp_df.join(hospital_beds_df, "county_uc", "left_outer")
           .na.fill({"hospital_beds": 0}))
county_df = tmp2_df.join(county_financial_df, "county_uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join cases with county information
# MAGIC 
# MAGIC After joining, count the number of cases from the Rural counties.
# MAGIC There are none because the data has been de-identified.

# COMMAND ----------

case_with_county_info_df = (case_df.join(county_df, case_df.res_county == county_df.county_uc, "left_outer")
                            .na.fill({"type": "Unknown"}))
display(case_with_county_info_df.filter(col("type") == "Rural").count())

# COMMAND ----------

# count cases with no county info
display(case_with_county_info_df[case_with_county_info_df.county.isNull()].count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis

# COMMAND ----------

# plot cases by month by county type
cases_bycounty_type_df = case_with_county_info_df.groupBy("case_month", "type").count()
display(cases_bycounty_type_df.orderBy(col("case_month")))

# COMMAND ----------

# get the number of counties by type that have cases for each month
distinct_counties_with_cases_bymonth_df = case_with_county_info_df.select("case_month", "res_county", "type").distinct()
county_count_with_cases_bymonth_df = distinct_counties_with_cases_bymonth_df.groupBy("case_month", "type").count()

# COMMAND ----------

# stacked bar graph of county count per type
display(county_count_with_cases_bymonth_df.orderBy("case_month"))

# COMMAND ----------

# Build list of 5 biggest counties in "Mix" type
biggest_mix_counties = []
for c in county_df.filter(col("type") == "Mix").orderBy("population", ascending=False).select("county_uc").head(5):
    biggest_mix_counties.append(c.county_uc)
biggest_mix_counties

# COMMAND ----------

# pull out the 5 biggest Mix counties
huge_mix_df = distinct_counties_with_cases_bymonth_df[distinct_counties_with_cases_bymonth_df.res_county.isin(biggest_mix_counties)]
# shows that these 5 counties are present in all months (except Winona in 5/2020)
display(huge_mix_df.groupBy("case_month", "res_county").count().orderBy("case_month"))

# COMMAND ----------

# drop cases from the biggest Mix counties from dataset
filtered_case_df = case_with_county_info_df[~case_with_county_info_df.res_county.isin(biggest_mix_counties)]

# and split into urban cases and non-urban cases
urban_case_df = filtered_case_df.filter(col("type") == "Urban")
non_urban_case_df = (filtered_case_df.filter(col("type") != "Urban")
                     .drop("type")
                     .withColumn("type", lit("Non-Urban")))

# COMMAND ----------

# MAGIC %md
# MAGIC Need total population, population for urban and non-urban counties

# COMMAND ----------

# get total population by county type
total_population = county_df.select("population").groupBy().sum().first()[0]
print(f"Total Population = {total_population}")

total_population_by_county_type_df = county_df.groupBy("type").sum("population").withColumnRenamed("sum(population)", "population")
print("Population for county type")
display(total_population_by_county_type_df)

total_urban_population = total_population_by_county_type_df.filter(col("type") == "Urban").select("population").groupBy().sum().first()[0]
print(f"Total Urban Population = {total_urban_population}")

excluded_population = county_df[county_df.county_uc.isin(biggest_mix_counties)].select("population").groupBy().sum().first()[0]
total_non_urban_population = total_population_by_county_type_df.filter(col("type") != "Urban").select("population").groupBy().sum().first()[0] - excluded_population
print(f"Total Non-Urban Population = {total_non_urban_population} (excluded counties = {excluded_population})")

assert total_population == total_urban_population + total_non_urban_population + excluded_population

# COMMAND ----------

# MAGIC %md
# MAGIC Count cases per month for Urban and Non-Urban counties
# MAGIC 
# MAGIC Calculate cases per 1000 county residents to make comparisons

# COMMAND ----------

# count cases per month per urban/non-urban
urban_cases_per_month_df = urban_case_df.groupBy("case_month", "type").count()
non_urban_cases_per_month_df = non_urban_case_df.groupBy("case_month", "type").count()

# COMMAND ----------

# calculate cases per thousand residents
urban_cases_per_thousand_df = (urban_cases_per_month_df
                               .withColumn("population", lit(total_urban_population))
                               .withColumn("cases_per_1000", 1000 * col("count") / col("population")))
non_urban_cases_per_thousand_df = (non_urban_cases_per_month_df
                                   .withColumn("population", lit(total_non_urban_population))
                                   .withColumn("cases_per_1000", 1000 * col("count") / col("population")))

# COMMAND ----------

# concatenate urban and non-urban cases with union
cases_per_thousand_df = urban_cases_per_thousand_df.union(non_urban_cases_per_thousand_df)
display(cases_per_thousand_df.orderBy("case_month"))

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate hospitalization percentage by Urban/Non-Urban counties

# COMMAND ----------

urban_hosp_case_df = urban_case_df.groupBy("case_month", "type").pivot("hosp_yn").count()
urban_hosp_percent_df = (urban_hosp_case_df
                         .withColumn("hosp_pct", 100 * col("YES") / (col("YES") + col("NO"))))

non_urban_hosp_case_df =  non_urban_case_df.groupBy("case_month", "type").pivot("hosp_yn").count()
non_urban_hosp_percent_df = (non_urban_hosp_case_df
                             .withColumn("hosp_pct", 100 * col("YES") / (col("YES") + col("NO"))))

cases_hosp_percent_df = urban_hosp_percent_df.union(non_urban_hosp_percent_df)

# COMMAND ----------

# take a look at the result
display(cases_hosp_percent_df.orderBy("case_month"))

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate mortality percentage by Urban/Non-Urban counties

# COMMAND ----------

urban_death_case_df = urban_case_df.groupBy("case_month", "type").pivot("death_yn").count()
urban_death_percent_df = (urban_death_case_df
                          .withColumn("death_pct", 100 * col("YES") / (col("YES") + col("NO"))))

non_urban_death_case_df =  non_urban_case_df.groupBy("case_month", "type").pivot("death_yn").count()
non_urban_death_percent_df = (non_urban_death_case_df
                              .withColumn("death_pct", 100 * col("YES") / (col("YES") + col("NO"))))

cases_death_percent_df = urban_death_percent_df.union(non_urban_death_percent_df)

# COMMAND ----------

# take a look at the result
display(cases_death_percent_df.orderBy("case_month"))

# from published data on https://www.health.state.mn.us/diseases/coronavirus/situation.html#death1
# cumulative deaths as of 2/28/2022 was 12,288 so we're only able to classify about 45%

# COMMAND ----------

# MAGIC %md
# MAGIC Get county info population, hospital bed totals by urban/non-urban
# MAGIC  - TODO: sum population, beds but average median income and uninsured rate

# COMMAND ----------

county_by_type_df = county_df[~county_df.county_uc.isin(biggest_mix_counties)].withColumn("type", when(col("type") == "Urban", "Urban").otherwise("Non-Urban"))
county_by_type_info_df = county_by_type_df.groupBy("type").sum().withColumnRenamed("sum(population)", "population").withColumnRenamed("sum(hospital_beds)", "hospital_beds")
display(county_by_type_info_df)

# COMMAND ----------

hosp_beds_used_df = (cases_hosp_percent_df
                     .join(county_by_type_info_df, cases_hosp_percent_df.type == county_by_type_info_df.type)
                     .withColumn("beds_used_pct", 100 * col("YES") / col("hospital_beds")))
display(hosp_beds_used_df.orderBy("case_month"))

# COMMAND ----------

# MAGIC %md
# MAGIC - Look into the relationship between median income and uninsured rate
# MAGIC - uninsured rate vs hospitalization?
# MAGIC - median income vs deaths?
# MAGIC - what else?

# COMMAND ----------

# scatter plot of median income vs uninsured rate
display(county_df)
