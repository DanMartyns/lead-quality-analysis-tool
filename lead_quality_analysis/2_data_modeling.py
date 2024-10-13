import os
import pyspark.sql.functions as F
from spark_controller.controller import SparkController, Dataframe

print("""
 ____        _          __  __           _      _ _             
|  _ \  __ _| |_ __ _  |  \/  | ___   __| | ___| (_)_ __   __ _ 
| | | |/ _` | __/ _` | | |\/| |/ _ \ / _` |/ _ \ | | '_ \ / _` |
| |_| | (_| | || (_| | | |  | | (_) | (_| |  __/ | | | | | (_| |
|____/ \__,_|\__\__,_| |_|  |_|\___/ \__,_|\___|_|_|_| |_|\__, |
                                                          |___/ 
""")

# This project will be built upon a star schema architecture, which 
# is widely used in data warehousing and business intelligence systems.

# Fact tables:
# The core of the star schema consists of fact tables that store
# quantitative data for analysis. These tables typically contain 
# measures, metrics, or facts that are subject to aggregation.

# Dimensions:
# Surrounding the fact tables are dimension tables, which provide 
# descriptive attributes related to the facts. These tables contain 
# categorical data that can be used to filter, group, and label 
# the measures in the fact tables.

controller = SparkController()

# Register the UDF
deterministic_uuid_udf = controller.generate_deterministic_uuid_udf()

######################################################################
##################### Get information from STEP 1 ####################
######################################################################

# Read information from the STEP 1
leads = controller.read_parquet_files(folder_path='step1/leads/')
leads.createOrReplaceTempView('leads')
reports = controller.read_parquet_files(folder_path='step1/reports/')
reports.createOrReplaceTempView('reports')

######################################################################
######################## Build Date Dimension ########################
######################################################################

# Define the start and end dates
start_date = "2000-01-01"
end_date = "2030-12-31"

# Create a temporary view to generate a date range
date_range_df = controller.spark.sql(f"""
    SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS date_key
""")
date_range_df.createOrReplaceTempView('date_range')

# Create the Date Dimension with additional attributes
date_dimension_sql = """
SELECT date_key,
        year(date_key) AS year,
        month(date_key) AS month,
        day(date_key) AS day,
        quarter(date_key) AS quarter,
        dayofweek(date_key) AS day_of_week,
        date_format(date_key, 'MMMM') AS month_name,  -- Full month name
        date_format(date_key, 'E') AS day_name         -- Abbreviated day name
FROM date_range
"""

# Create the Date Dimension DataFrame
date_dimension_df = controller.spark.sql(date_dimension_sql)
date_dimension_df = date_dimension_df.withColumn("uuid", deterministic_uuid_udf(*[F.col("date_key")]))
date_dimension_df.createOrReplaceTempView('d_date')

controller.logging.info("Date Dimension")
date_dimension_df.show(10)
date_dimension_df.write.mode("overwrite").parquet("step2/date/")

######################################################################
####################### Build Lead Dimension #########################
######################################################################

leads_dimension_sql = f"""
        SELECT DISTINCT
                leads.lead_uuid AS uuid,
                reports.lead_number,
                leads.phone_hash,
                leads.email_hash
        FROM leads
        LEFT JOIN reports
                ON leads.email_hash = reports.email_hash
                AND leads.phone_hash = reports.phone_hash
"""

# Create the Leads Dimension DataFrame
leads_dimension = controller.spark.sql(leads_dimension_sql)
leads_dimension.createOrReplaceTempView('d_leads')

controller.logging.info("Leads Dimension")
leads_dimension.show(10)
leads_dimension.write.mode("overwrite").parquet('step2/leads/')

######################################################################
###################### Build Place Dimension #########################
######################################################################

places_dimension_sql = f"""
        SELECT DISTINCT
                city_name,
                zip,
                state
        FROM reports
"""

# Create the Places Dimension DataFrame
places_dimension = controller.spark.sql(places_dimension_sql)
places_dimension = places_dimension.withColumn("uuid", deterministic_uuid_udf(*[F.col("city_name"), F.col("zip"), F.col("state")]))
places_dimension.createOrReplaceTempView('d_places')

controller.logging.info("Places Dimension")
places_dimension.show(10)
places_dimension.write.mode("overwrite").parquet('step2/places/')

######################################################################
##################### Build Status Dimension #########################
######################################################################

status_dimension_sql = f"""SELECT DISTINCT job_status FROM reports"""

# Create the Status Dimension DataFrame
status_dimension = controller.spark.sql(status_dimension_sql)
status_dimension = status_dimension.withColumn("uuid", deterministic_uuid_udf(*[F.col("job_status")]))
status_dimension.createOrReplaceTempView('d_status')

controller.logging.info("Status Dimension")
status_dimension.show(10)
status_dimension.write.mode("overwrite").parquet('step2/status/')

######################################################################
################### Build KPI Calculation Fact Table #################
######################################################################

kpi_calculation_sql = f"""
        SELECT
                reports.entry_date,
                d_leads.uuid AS lead_uuid,
                d_places.uuid AS place_uuid,
                d_status.uuid AS status_uuid,
                d_date.uuid AS date_uuid,
                SUBSTRING(reports.appt_date, 12, 8) AS appointment_time,                
                reports.demo,
                reports.set,
                reports.dispo
        FROM reports
        LEFT JOIN d_leads
                ON d_leads.email_hash = reports.email_hash
                AND d_leads.phone_hash = reports.phone_hash
                AND d_leads.lead_number = reports.lead_number      
        LEFT JOIN d_places
                ON d_places.city_name = reports.city_name
                AND d_places.zip = reports.zip
                AND d_places.state = reports.state
        LEFT JOIN d_date
                ON d_date.date_key = CAST(reports.appt_date AS DATE)
        LEFT JOIN d_status
                ON d_status.job_status = reports.job_status        
"""

# Create the KPI Calculation DataFrame
kpi_calculation = controller.spark.sql(kpi_calculation_sql)
kpi_calculation = kpi_calculation.withColumn("uuid", deterministic_uuid_udf(*[
        F.col("entry_date"),
        F.col("lead_uuid"), 
        F.col("place_uuid"),
        F.col("status_uuid"),         
        F.col("date_uuid"), 
        F.col("appointment_time"), 
        F.col("demo"), 
        F.col("set"), 
        F.col("dispo")
]))

controller.logging.info("KPI Calculation Fact Table")
kpi_calculation.show(10)
kpi_calculation.write.mode("overwrite").parquet('step2/kpi_calculation/')