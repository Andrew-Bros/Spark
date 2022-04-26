# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Project: COVID-19 - The Urban/Rural Divide
# MAGIC Andrew Bros
# MAGIC SENG-5709
# MAGIC 
# MAGIC ## Imports and constants

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import col, upper

S3BUCKET = "s3://seng-5709-spark"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define schema and load datasets
# MAGIC 
# MAGIC ### CDC case data with geography
# MAGIC https://data.cdc.gov/Case-Surveillance/COVID-19-Case-Surveillance-Public-Use-Data-with-Ge/n8mc-b4w4
# MAGIC 
# MAGIC   - Using unix tools, extracted the header and rows for res_state = MN to reduce the dataset from 69 to 1.4 million rows.

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
case_df = spark.read.format("csv").options(header="true", dateFormat="YYYY-mm").schema(case_schema).load(f"{S3BUCKET}/COVID-MN.csv")
case_df.show(5, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### County population
# MAGIC https://data.census.gov/cedsci/table?t=Population%20Total&g=0400000US27%240500000&tid=ACSDT5Y2020.B01003&tp=true
# MAGIC 
# MAGIC   - Extracted as Excel spreadsheet then saved as csv with just county, population

# COMMAND ----------

population_schema = StructType([
    StructField("County", StringType(), False),
    StructField("Population", IntegerType(), False),
    ])
population_df = spark.read.format("csv").options(header="true").schema(population_schema).load(f"{S3BUCKET}/county_population.csv")
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
    StructField("County", StringType(), False),
    StructField("Type", StringType(), False),
    ])
county_type_df = spark.read.format("csv").options(header="true").schema(county_type_schema).load(f"{S3BUCKET}/county.csv")
display(county_type_df.groupBy("Type").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### MN Department of Health, County profiles
# MAGIC https://data.web.health.state.mn.us/web/mndata/download-county-data
# MAGIC 
# MAGIC   - took a while to figure out that this file is utf-16 encoded
# MAGIC   - symptom was that State Value and Value could not be converted to double

# COMMAND ----------

profile_schema = StructType([
    StructField("County", StringType(), False),
    StructField("Indicator category", StringType(), False),
    StructField("Indicator", StringType(), False),
    StructField("Year", StringType(), True),
    StructField("State Value", DoubleType(), False),
    StructField("Value", DoubleType(), False),
    StructField("Unit of measure", StringType(), False),
    ])
profile_df = spark.read.format("csv").options(encoding="utf-16", header="true", sep="\t").schema(profile_schema).load(f"{S3BUCKET}/MN_county_profile.csv")
# needs additional transformation to be useful
profile_df.show(5, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### MN Department of Health provider list
# MAGIC https://mdhprovidercontent.web.health.state.mn.us/showprovideroutput.cfm
# MAGIC 
# MAGIC   - download of xls, saved as csv

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
provider_df = spark.read.format("csv").options(header="true").schema(provider_schema).load(f"{S3BUCKET}/provider_list.csv")
provider_df.show(5, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations
# MAGIC 
# MAGIC   - join county type and population and add uppercase county name to make joins easier
# MAGIC   - count cases by month and county

# COMMAND ----------

county_df = county_type_df.join(population_df, "County").withColumn("county_uc", upper(col("County")))
case_count_by_county_df = case_df.groupBy("case_month", "res_county").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis
# MAGIC 
# MAGIC   - join cases to county type

# COMMAND ----------

case_county_type_df = case_df.join(county_df, case_df.res_county == county_df.county_uc, "left_outer").na.fill({"Type": "Unknown"})
cases_bymonth_bycounty_type_df = case_county_type_df.groupBy("case_month", "Type").count()

# COMMAND ----------
# MAGIC %md
# MAGIC ## just trying to figure things out

# COMMAND ----------

display(case_count_by_county_df.filter(col("res_county") == "NA"))

# COMMAND ----------

case_county_type_df.tail(10)

# COMMAND ----------

display(county_df)

# COMMAND ----------

display(county_type_df.groupBy("Type").count())

# COMMAND ----------

display(cases_bymonth_bycounty_type_df.orderBy(col("case_month")))

# COMMAND ----------

df = case_county_type_df.select("case_month", "res_county", "Type")
df.show(5, False)

# COMMAND ----------

maybe = df.groupBy("case_month", "res_county")
doubtful = maybe.pivot("Type").count()
doubtful.show(5, False)

# COMMAND ----------

doubtful.count()
