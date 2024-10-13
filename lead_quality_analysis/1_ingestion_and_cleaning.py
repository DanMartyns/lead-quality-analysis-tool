import os
from spark_controller.controller import SparkController, Dataframe
import pyspark.sql.functions as F

# Initalize Spark Controller
controller = SparkController()

###############################################################################################################
#################################### Extraction/Cleaning/Standarization #######################################
###############################################################################################################
print("""
 ___                       _   _                               _ 
|_ _|_ __   __ _  ___  ___| |_(_) ___  _ __     __ _ _ __   __| |
 | || '_ \ / _` |/ _ \/ __| __| |/ _ \| '_ \   / _` | '_ \ / _` |
 | || | | | (_| |  __/\__ \ |_| | (_) | | | | | (_| | | | | (_| |
|___|_|_|_|\__, |\___||___/\__|_|\___/|_| |_|  \__,_|_| |_|\__,_|
 / ___| | _|___/_ _ _ __ (_)_ __   __ _                          
| |   | |/ _ \/ _` | '_ \| | '_ \ / _` |                         
| |___| |  __/ (_| | | | | | | | | (_| |                         
 \____|_|\___|\__,_|_| |_|_|_| |_|\__, |                         
                                  |___/                          
""")

# Extract information of the Leads
folder_path = os.getenv('LEADS_PREFIX', 'leads')

dataframes = []
for filename in os.listdir(folder_path):
        print()
        dataframe = controller.read_parquet_file(folder_path=folder_path, filename=filename)
        dataframe = Dataframe(dataframe=dataframe)\
                .clean_dataframe()\
                .enforce_schema(schema_name='leads')\
                .to_dataframe()
        print()

        dataframes.append(dataframe)

leads = controller.concatenate(dataframes=dataframes)

# leads.createOrReplaceTempView('leads')
# Confirmation that we have one lead_uuid for each email_hash and phone_hash
# controller.spark.sql(f"""
#        SELECT
#                email_hash,
#                phone_hash,
#                COUNT(DISTINCT lead_uuid) AS unique_lead_uuids,
#                COUNT(*) AS total_rows,
#                COUNT(CASE WHEN lead_uuid IS NULL THEN 1 END) AS null_lead_uuid_count,
#                COUNT(CASE WHEN lead_uuid IS NOT NULL THEN 1 END) AS non_null_lead_uuid_count
#        FROM leads
#        GROUP BY email_hash, phone_hash
#""").show()

controller.logging.info("Schema:")
leads.printSchema()

controller.logging.info("[INFO] Sample:")
leads.show(10)

controller.logging.info("[INFO] Saving Leads Standarized")
leads.write.mode("overwrite").parquet('step1/leads/')

# Extract information from Client Reports
folder_path = os.getenv('REPORTS_PREFIX', 'contractor_reports')

dataframes = []
for filename in os.listdir(folder_path):
        print()
        dataframe = controller.read_csv_file(folder_path=folder_path, filename=filename)
        dataframe = Dataframe(dataframe=dataframe)\
                .clean_dataframe()\
                .enforce_schema(schema_name='reports')\
                .clean_invalid_values("email_hash", invalid_values=['-----'])\
                .clean_invalid_values("phone_hash", invalid_values=['-----'])\
                .clean_invalid_values("dispo", invalid_values=['-----'])\
                .clean_invalid_values("job_status", invalid_values=['-----'])\
                .clean_invalid_values("city_name", invalid_values=['-----'])\
                .to_dataframe()
        print()

        dataframes.append(dataframe)

reports = controller.concatenate(dataframes=dataframes)
reports.createOrReplaceTempView('reports')

controller.logging.info("Schema:")
reports.printSchema()

controller.logging.info("Sample:")
reports.show(10)

controller.logging.info("Saving Reports Standarized")
reports.write.mode("overwrite").parquet('step1/reports/')

"""
# Queries used for pre-analysis

# Count the total number of rows
total_rows = reports.count()
# Count the number of rows with missing values for each column
missing_values_per_column = reports.select([F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column) for column in reports.columns])
missing_values_per_column.show()
# Calculate the percentage of missing values for each column
missing_percentage = missing_values_per_column.select([F.col(column) / total_rows * 100 for column in missing_values_per_column.columns])        
missing_percentage.show()

# Check if each email hash and phone hash has only one lead number
controller.spark.sql('''
WITH check AS (
        SELECT 
                email_hash,
                phone_hash,
                CARDINALITY(ARRAY_AGG(lead_number)) AS lead_numbers
        FROM reports
        GROUP BY 1,2
        HAVING CARDINALITY(ARRAY_AGG(lead_number)) > 1
)
SELECT COUNT(*) FROM check read
''').show()
"""