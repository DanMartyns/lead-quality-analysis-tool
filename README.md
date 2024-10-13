# Lead Quality Analysis Tool
This project is designed to match data from our backend system with the client's CRM reports to analyze and improve lead quality. The process runs twice daily and calculates key metrics such as the conversion rate from delivered leads to homeowner appointments and the progression of leads in the sales process.

## Problem Overview
- **Parquet File**: Contains leads delivered to the client, exported from our backend system.
- **Client Reports**: The client manually creates reports with lead progress and uploads them to an SFTP server as CSV snapshots.
- **Matching Data**: The only link between our leads and the client's reports is the hashed email and phone numbers.
- **Key Metrics**:
    - **Conversion Rate**: From delivered leads to appointments (tracked in the *set* column).
    - **Appointments**: Scheduled appointment dates (tracked in the *appt date* column).
    - **Demos**: Whether an appointment resulted in a demo, the next step in the sales process.

## Project Stages
1. **Data Ingestion and Cleaning**
    - Load the Parquet file containing lead data.
    - Load CSV snapshots from the client's CRM.
    - Standardize the data by cleaning and preparing it for matching.
2. **Data Matching and Modeling**
    - Match leads based on hashed email and phone numbers.
    - Create a data model to link the two datasets.
3. **Analysis and Metrics Calculation**
    - Calculate conversion rates for lead quality analysis.
    - Track progress through appointments (set column) and demos.

## Project Structure
```bash
LEAD_QUALITY_ANALYSIS/
│
├── source/
│   ├── contractor_reports/   # Contains client report data in CSV format
│   └── leads/                # Contains leads data in Parquet format
│
├── spark_controller/
│   ├── __init__.py           # Package initialization
│   ├── controller.py         # Utils script with useful functions to help the process
│   └── schemas.json          # JSON file with schema definitions
│
├── 1_ingestion_and_cleaning.py     # Data Ingestion and Cleaning logic
├── 2_data_modeling.py              # Data Matching and Modeling logic
├── 3_analysis_and_calculations.py  # Analysis and Metrics Calculation logic
│
├── docker-compose.yml         # Docker Compose setup for containerized execution
├── Dockerfile                 # Docker image definition
├── requirements.txt           # Project dependencies
```