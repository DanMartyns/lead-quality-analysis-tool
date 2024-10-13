# Use an official Python runtime as a parent image
FROM jupyter/pyspark-notebook

# Set the working directory in the container
WORKDIR /lead_quality_analysis
ENV PYTHONPATH /lead_quality_analysis

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY 1_ingestion_and_cleaning.py .
COPY 2_data_modeling.py .
COPY 3_analysis_and_calculations.py .

# Copy Controller with util functions
COPY spark_controller/ ./spark_controller/

# Copy the Source Data with the leads and Client Reports
COPY source/ ./source/

# Create a folder to persist the output after Ingestion and Cleaning
RUN mkdir -p step1/

# Create a folder to persist the output after Data Modeling
RUN mkdir -p step2/

# This will run 1_ingestion_and_cleaning.py first, and then 2_data_modeling.py, and finally 3_analysis_and_calculations
CMD ["sh", "-c", "python 1_ingestion_and_cleaning.py && python 2_data_modeling.py && python 3_analysis_and_calculations.py"]