from spark_controller.controller import SparkController

print("""
    _                _           _                       _ 
   / \   _ __   __ _| |_   _ ___(_)___    __ _ _ __   __| |
  / _ \ | '_ \ / _` | | | | / __| / __|  / _` | '_ \ / _` |
 / ___ \| | | | (_| | | |_| \__ \ \__ \ | (_| | | | | (_| |
/_/   \_\_| |_|\__,_|_|\__, |___/_|___/  \__,_|_| |_|\__,_|
  ____      _          |___/     _   _                     
 / ___|__ _| | ___ _   _| | __ _| |_(_) ___  _ __  ___     
| |   / _` | |/ __| | | | |/ _` | __| |/ _ \| '_ \/ __|    
| |__| (_| | | (__| |_| | | (_| | |_| | (_) | | | \__ \    
 \____\__,_|_|\___|\__,_|_|\__,_|\__|_|\___/|_| |_|___/    
""")

controller = SparkController()

d_dates = controller.read_parquet_files('./step2/date/')
d_dates.createOrReplaceTempView('d_dates')

d_leads = controller.read_parquet_files('step2/leads/')
d_leads.createOrReplaceTempView('d_leads')

d_places = controller.read_parquet_files('./step2/places/')
d_places.createOrReplaceTempView('d_places')

d_status = controller.read_parquet_files('./step2/status/')
d_status.createOrReplaceTempView('d_status')

kpi_calculation = controller.read_parquet_files('./step2/kpi_calculation/')
kpi_calculation.createOrReplaceTempView('kpi_calculation')

# Understand the Global Feedback
query = """
  WITH calculation AS (
    SELECT
      COUNT(*) AS Total_Delivered_Leads,
      COUNT(*) FILTER (WHERE set = true) AS Total_Scheduled_Appointments,
      COUNT(*) FILTER (WHERE set = true AND demo = true) AS Total_Demos
    FROM kpi_calculation kpi
  )
  SELECT 
      Total_Delivered_Leads,
      Total_Scheduled_Appointments,
      Total_Demos,
      COALESCE(
          ROUND(
          ((Total_Scheduled_Appointments / Total_Delivered_Leads) * 100)
          , 2)
      , 0) AS Conversion_Rate,
      COALESCE(
          ROUND(
          ((Total_Demos / Total_Scheduled_Appointments) * 100)
          , 2)
      , 0) AS Demo_Conversion_Rate
  FROM calculation  
"""
metrics = controller.spark.sql(query)

# Understand the Feedback by State
query = """
  WITH calculation AS (
    SELECT
      d_places.state,
      d_places.city_name,
      COUNT(*) AS Total_Delivered_Leads,
      COUNT(*) FILTER (WHERE set = True) AS Total_Scheduled_Appointments,
      COUNT(*) FILTER (WHERE set = True AND demo = True) AS Total_Demos
    FROM kpi_calculation
    LEFT JOIN d_places
      ON kpi_calculation.place_uuid = d_places.uuid
    GROUP BY 1,2
  ),
  top_cities AS (
    SELECT 
        state, 
        city_name, 
        Total_Delivered_Leads,
        Total_Scheduled_Appointments,
        Total_Demos,
        COALESCE(
            ROUND(
            ((Total_Scheduled_Appointments / Total_Delivered_Leads) * 100)
            , 2)
        , 0) AS Conversion_Rate,
        COALESCE(
            ROUND(
            ((Total_Demos / Total_Scheduled_Appointments) * 100)
            , 2)
        , 0) AS Demo_Conversion_Rate        
    FROM calculation 
    ORDER BY Total_Delivered_Leads DESC 
    LIMIT 5
  ),
  bottom_cities AS (
      SELECT 
          state, 
          city_name, 
          Total_Delivered_Leads,
          Total_Scheduled_Appointments,
          Total_Demos,
          COALESCE(
              ROUND(
              ((Total_Scheduled_Appointments / Total_Delivered_Leads) * 100)
              , 2)
          , 0) AS Conversion_Rate,
          COALESCE(
              ROUND(
              ((Total_Demos / Total_Scheduled_Appointments) * 100)
              , 2)
          , 0) AS Demo_Conversion_Rate          
      FROM calculation 
      ORDER BY Total_Delivered_Leads ASC 
      LIMIT 5
  )
  SELECT * FROM top_cities
  UNION ALL
  SELECT * FROM bottom_cities
"""

metrics_by_state = controller.spark.sql(query)

# Check the amount of transactions by date
query = """
  WITH calculation AS (
    SELECT
      YEAR(d_dates.date_key) AS year,
      MONTH(d_dates.date_key) AS month,
      COUNT(*) AS Total_Delivered_Leads,
      COUNT(*) FILTER (WHERE set = True) AS Total_Scheduled_Appointments,
      COUNT(*) FILTER (WHERE set = True AND demo = True) AS Total_Demos
    FROM kpi_calculation
    LEFT JOIN d_dates
      ON kpi_calculation.date_uuid = d_dates.uuid
    GROUP BY 1,2
  )
  SELECT
      year,
      month,
      Total_Delivered_Leads,
      Total_Scheduled_Appointments,
      Total_Demos,
      COALESCE(
          ROUND(
          ((Total_Scheduled_Appointments / Total_Delivered_Leads) * 100)
          , 2)
      , 0) AS Conversion_Rate,
      COALESCE(
          ROUND(
          ((Total_Demos / Total_Scheduled_Appointments) * 100)
          , 2)
      , 0) AS Demo_Conversion_Rate
  FROM calculation
  ORDER BY Total_Delivered_Leads
"""
metrics_by_entry_date = controller.spark.sql(query)

query = """
  WITH calculation AS (
    SELECT
      d_status.job_status,
      COUNT(*) AS Total_Delivered_Leads,
      COUNT(*) FILTER (WHERE set = True) AS Total_Scheduled_Appointments,
      COUNT(*) FILTER (WHERE set = True AND demo = True) AS Total_Demos
    FROM kpi_calculation
    LEFT JOIN d_status
      ON kpi_calculation.status_uuid = d_status.uuid
    GROUP BY 1
  )
  SELECT
      job_status,
      Total_Delivered_Leads,
      Total_Scheduled_Appointments,
      Total_Demos,
      COALESCE(
          ROUND(
          ((Total_Scheduled_Appointments / Total_Delivered_Leads) * 100)
          , 2)
      , 0) AS Conversion_Rate,
      COALESCE(
          ROUND(
          ((Total_Demos / Total_Scheduled_Appointments) * 100)
          , 2)
      , 0) AS Demo_Conversion_Rate
  FROM calculation
  ORDER BY Total_Delivered_Leads
"""
metrics_by_status = controller.spark.sql(query)

query = """
  WITH calculation AS (
    SELECT
      d_places.city_name, 
      d_status.job_status,
      COUNT(*) AS Total_Delivered_Leads,
      COUNT(*) FILTER (WHERE set = True) AS Total_Scheduled_Appointments,
      COUNT(*) FILTER (WHERE set = True AND demo = True) AS Total_Demos
    FROM kpi_calculation
    JOIN d_status
      ON kpi_calculation.status_uuid = d_status.uuid
    LEFT JOIN d_places
      ON kpi_calculation.place_uuid = d_places.uuid
    GROUP BY 1,2
  )
  SELECT
      city_name,
      job_status,
      Total_Delivered_Leads,
      Total_Scheduled_Appointments,
      Total_Demos,
      COALESCE(
          ROUND(
          ((Total_Scheduled_Appointments / Total_Delivered_Leads) * 100)
          , 2)
      , 0) AS Conversion_Rate,
      COALESCE(
          ROUND(
          ((Total_Demos / Total_Scheduled_Appointments) * 100)
          , 2)
      , 0) AS Demo_Conversion_Rate
  FROM calculation
  ORDER BY Total_Delivered_Leads
"""
metrics_by_status_and_city = controller.spark.sql(query)

controller.logging.info("Global Feedback")
metrics.show(10)

controller.logging.info("Metrics by State")
metrics_by_state.show(1000)

controller.logging.info("Metrics by Entry Date")
metrics_by_entry_date.show(1000)

controller.logging.info("Metrics by Job Status")
metrics_by_status.show(1000)

controller.logging.info("Metrics by Job Status and City")
metrics_by_status_and_city.show(1000)