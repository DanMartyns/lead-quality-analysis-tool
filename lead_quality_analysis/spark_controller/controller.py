import os
import re
import sys
import json
import logging
from functools import reduce
from datetime import datetime
from typing import List, Optional
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DateType, TimestampType
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, concat_ws
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (can also use DEBUG, ERROR, etc.)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Format with timestamps
    handlers=[
        logging.StreamHandler(sys.stdout)  # Log to stdout to be captured by Docker
    ]
)

class Dataframe:

    def __init__(self, dataframe: DataFrame):
        self.dataframe = dataframe
        self.logging = logging.getLogger(__name__)
        
        schema_path = './spark_controller/schemas.json'
        # Open and read the JSON file
        with open(schema_path, 'r') as file:
            self.schema = json.load(file)

    def __get_schema(self, schema_name: str) -> dict:
        """
        Retrieves the schema definition for a specified schema name from the schemas.json

        This method searches for the schema with the given name within the 
        'input_schemas' attribute of the instance. If the schema is found, 
        it returns the corresponding schema definition as a dictionary. 
        If the schema does not exist, a ValueError is raised.

        Args:
            schema_name (str): The name of the schema to retrieve.

        Returns:
            dict: A dictionary representing the schema definition.

        Raises:
            ValueError: If the specified schema name is not found in the input schemas.
        """

        # Search for the specified schema in the input schemas
        table_schema = next(
            (table for table in self.schema.get('input_schemas', []) if table['name'] == schema_name),
            None
        )

        # Raise an error if the schema is not found
        if table_schema is None:
            raise ValueError(f"Schema '{schema_name}' not found in the provided schema.")

        # Return the found schema definition
        return table_schema

    def clean_dataframe(self) -> "Dataframe":
        """
        Cleans a Spark DataFrame by performing the following transformations:
        - 1. Trims white spaces from string columns.
        - 2. Drop duplicate rows
        - 3. Drops rows where all columns are NULL.

        Args:
            dataframe (pyspark.sql.DataFrame): Spark DataFrame to clean.

        Returns:
            pyspark.sql.DataFrame: Cleaned Spark DataFrame.
        """
        self.logging.info("Starting cleaning the dataframe")
        for column in self.dataframe.columns:
            
            # 1. Trim leading/trailing and remove spaces in the middle
            self.dataframe = self.dataframe.withColumn(column, F.trim(F.regexp_replace(F.col(column), r"\s+", " ")))

        # 2. Remove duplicates based on all columns
        self.dataframe = self.dataframe.dropDuplicates()

        # 3. Drops rows where all columns are NULL.
        self.dataframe = self.dataframe.na.drop(how='all')

        self.logging.info("Finishing cleaning the dataframe")
        return self

    def clean_invalid_values(self, column: str, invalid_values: List[str]) -> "Dataframe":
        """
        Replaces specified invalid values in a column with NULL.
        
        Args:
            column (str): The name of the column to clean.
            invalid_values (list): A list of values considered invalid.
        
        Returns:
            DataframeController: Returns the updated DataframeController with cleaned data.
        """
        self.logging.info(f"Starting cleaning column {column} for invalid values [{', '.join(invalid_values)}]")
        # Initialize the `when` condition to replace invalid values with NULL
        condition = F.when(F.col(column).isin(invalid_values), None)
        
        # Return a new DataFrame with the invalid values replaced
        self.dataframe = self.dataframe.withColumn(column, condition.otherwise(F.col(column)))
        
        self.logging.info(f"Finishing cleaning column {column} for invalid values")
        return self

    def enforce_schema(self, schema_name: str, action: str = 'replace') -> "Dataframe":

        def enforce_column_names(dataframe: Dataframe, schema: dict) -> DataFrame:
            """
            Standardizes the column names of a Spark DataFrame according to a given schema.
            
            - If a column has a new name in the schema, use that name.
            - If no new name is provided, standardize the column name to snake_case.
            - Only retain columns defined in the schema, ignoring extra columns.
            
            Args:
            - dataframe (pyspark.sql.DataFrame): The input DataFrame whose columns need to be standardized.
            - schema (dict): The schema defining column names and their new names.

            Returns:
            - pyspark.sql.DataFrame: A DataFrame with standardized and enforced column names.
            """
            
            # Convert column names to snake_case
            def to_snake_case(col_name: str) -> str:
                """Converts a given column name to snake_case."""
                return re.sub(r'[^a-zA-Z0-9]+', '_', col_name.strip()).lower()

            self.logging.info("Starting the Standardization of Column Names")
            
            # Extract the schema entries
            entries = schema.get('entries', [])

            # Create a mapping for new column names (if defined), or use snake_case conversion
            new_name_map = {}
            for entry in entries:
                original_name = entry['name']
                # If the column name is a list, find the first matching column in the DataFrame
                if isinstance(original_name, list):
                    matched_col = next((col for col in original_name if col in dataframe.columns), None)
                    if matched_col:
                        original_name = matched_col
                    else:
                        self.logging.warning(f"Column '{original_name}' not found in the DataFrame. Skipping.")
                        continue
                
                # Map old name to new name if provided, else convert it to snake_case
                new_name_map[original_name] = entry.get('new_name', to_snake_case(original_name))

            # Rename the columns in the DataFrame and retain only those in the schema
            renamed_columns = [
                dataframe[col].alias(new_name_map[col]) for col in dataframe.columns if col in new_name_map
            ]

            self.logging.info("Finalizing the Standardization of Column Names")
            # Return the DataFrame with renamed columns, ignoring extra columns
            return dataframe.select(*renamed_columns)

        def enforce_data_types(dataframe: DataFrame, schema: dict, action: str) -> DataFrame:
            """
            Validates and casts the Spark DataFrame columns to the expected data types defined in the schema.
            
            Args:
            - dataframe (pyspark.sql.DataFrame): The input DataFrame whose columns need to be cast.
            - schema (dict): The schema definition for validation.
            
            Returns:
            - pyspark.sql.DataFrame: A DataFrame with columns cast to their expected types.
            """

            def enforce_date_type(df: DataFrame, column: str, format: str) -> DataFrame:
                """Enforces date type on a column, with optional format."""
                if format:
                    return df.withColumn(column, F.to_date(F.col(column), format))
                return df.withColumn(column, F.to_date(F.col(column)))            

            def enforce_timestamp_type(df: DataFrame, column: str, format: str) -> DataFrame:
                """Enforces timestamp type on a column, with optional format."""
                if format:
                    return df.withColumn(column, F.to_timestamp(F.col(column), format))
                return df.withColumn(column, F.to_timestamp(F.col(column)))

            def enforce_integer_type(df: DataFrame, column: str, format: str = None) -> DataFrame:
                """Enforces integer type on a column, with optional format validation."""
                if format:
                    return df.withColumn(
                        column, 
                        F.when(F.col(column).cast(StringType()).rlike(format), F.col(column).cast(IntegerType())).otherwise(None)
                    )
                return df.withColumn(column, F.col(column).cast(IntegerType()))

            def enforce_boolean_type(df: DataFrame, column: str) -> DataFrame:
                """Enforces boolean type on a column, considering common boolean representations."""
                valid_true = ['true', '1']
                valid_false = ['false', '0']

                return df.withColumn(
                    column,
                    F.when(F.lower(F.col(column)).isin(valid_true), F.lit(True))
                    .when(F.lower(F.col(column)).isin(valid_false), F.lit(False))
                    .otherwise(None)
                ) 

            def enforce_string_type(df: DataFrame, column: str, format: str = None) -> DataFrame:
                """Enforces string type on a column, with optional format validation."""
                if format:
                    return df.withColumn(
                        column, 
                        F.when(F.col(column).rlike(format), F.col(column).cast(StringType())).otherwise(None)
                    )
                return df.withColumn(column, F.col(column).cast(StringType()))

            self.logging.info("Starting Schema Verification and Enforcement")
            
            # Extract schema entries
            entries = schema.get('entries', [])

            # Loop through the schema to process each column
            for entry in entries:
                column_names = entry['name']
                expected_type = entry['type']
                expected_format = entry.get('format', None)

                # Support for multiple possible column names
                if isinstance(column_names, list):
                    # Find the first matching column in the DataFrame
                    matching_column = next((col for col in column_names if col in dataframe.columns), None)
                    if not matching_column:
                        self.logging.warning(f"None of the columns {column_names} were found in the DataFrame.")
                        continue
                else:
                    matching_column = column_names

                # If the column doesn't exist, skip it
                if matching_column not in dataframe.columns:
                    self.logging.warning(f"Column '{matching_column}' not found in the DataFrame.")
                    continue
                    
                # Keep original column for invalid value tracking
                dataframe = dataframe.withColumn(f'{matching_column}_orig', F.col(matching_column))

                # Determine the appropriate casting or transformation based on type
                if expected_type == 'date':
                    dataframe = enforce_date_type(dataframe, matching_column, expected_format)

                elif expected_type == 'timestamp':
                    dataframe = enforce_timestamp_type(dataframe, matching_column, expected_format)

                elif expected_type == 'integer':
                    dataframe = enforce_integer_type(dataframe, matching_column, expected_format)

                elif expected_type == 'boolean':
                    dataframe = enforce_boolean_type(dataframe, matching_column)

                else:
                    dataframe = enforce_string_type(dataframe, matching_column, expected_format)

                # Identify and warn about invalid values
                invalid_values_df = dataframe.filter(F.col(f"{matching_column}_orig").isNotNull() & F.col(matching_column).isNull())
                invalid_values = invalid_values_df.select(f"{matching_column}_orig").distinct().collect()
                invalid_values = [row[f"{matching_column}_orig"] for row in invalid_values]
                
                # Remove the original column used for invalid value tracking
                dataframe = dataframe.drop(f'{matching_column}_orig')

                # Display warning for columns with invalid values
                if invalid_values:
                    self.logging.warning(f"Format mismatch in column '{matching_column}': invalid values found - {invalid_values}")

                    if action == 'replace':
                        dataframe = dataframe.withColumn(matching_column, F.when(F.col(matching_column).isNull(), None).otherwise(F.col(matching_column)))
                        self.logging.info(f"Column '{matching_column}' had invalid values {invalid_values} replaced with NULL")
                    elif action == 'log':
                        pass 
                else:
                    self.logging.info(f"Column '{matching_column}' successfully cast to '{expected_type}'")

            self.logging.info("Finishing Schema Verification and Enforcement")
            return dataframe

        # Get the specified schema
        schema = self.__get_schema(schema_name=schema_name)

        self.dataframe = enforce_data_types(self.dataframe, schema, action)
        self.dataframe = enforce_column_names(self.dataframe, schema)
        
        return self

    def to_dataframe(self):
        return self.dataframe

class SparkController:

    def __init__(self):
        self.logging = logging.getLogger(__name__)

        # Initialize Spark Session
        self.spark = SparkSession.builder.getOrCreate()
        self.spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY") # In order to deal with datetime like 03/24/2023 2:00PM
        self.spark.sparkContext.setLogLevel("OFF")
        
        schema_path = './spark_controller/schemas.json'
        # Open and read the JSON file
        with open(schema_path, 'r') as file:
            self.schema = json.load(file)

    def read_csv_file(self, folder_path: str, filename: str) -> Optional[DataFrame]:
        """
        Reads a CSV file from the specified path into a Spark DataFrame.

        Args:
            folder_path (str): Path to the folder containing the CSV file.
            filename (str): Name of the CSV file.

        Returns:
            Optional[DataFrame]: The Spark DataFrame created from the CSV file or None
                                    if an error occurs during reading.
        """
        file_path = os.path.join(folder_path, filename)

        # Check file existence and extension
        if not os.path.exists(file_path) or not filename.endswith('.csv'):
            self.logging.error(f"File not found or not a CSV: {file_path}")
            return None
        
        try:
            # Read the CSV file
            dataframe = self.spark.read.csv(file_path, header=True)
            self.logging.info(f"Successfully read {dataframe.count()} rows from {file_path}")
            return dataframe
        except FileNotFoundError as e:
            self.logging.error(f"File not found: {file_path} - {e}")
            return None
        except Exception as e:
            self.logging.error(f"Error reading CSV file: {file_path} - {e}")
            return None

    def read_parquet_file(self, folder_path: str, filename: str) -> Optional[DataFrame]:
        """
        Reads a Parquet file from the specified folder into a Spark DataFrame.
        
        Args:
        - folder_path (str): The path to the folder containing the Parquet files.
        - filename (str): The name of the Parquet file to read.
        
        Returns:
        - Optional[DataFrame]: A Spark DataFrame if the file is successfully read, else None.
        """
        if not filename.endswith('.parquet'):
            self.logging.warning(f"Skipping non-parquet file: {filename}")
            return None

        file_path = os.path.join(folder_path, filename)
        
        try:
            dataframe = self.spark.read.parquet(file_path)
            self.logging.info(f"Successfully read Parquet file: {file_path}")
            return dataframe
        except Exception as e:
            self.logging.error(f"Error reading Parquet file {file_path}: {e}")
            return None

    def concatenate(self, dataframes: List[Optional[DataFrame]]) -> Optional[DataFrame]:
        """
        Concatenates a list of Spark DataFrames using `unionByName`, handling missing columns automatically.
        
        Args:
        - dataframes (List[Optional[DataFrame]]): List of DataFrames to concatenate.
        
        Returns:
        - Optional[DataFrame]: A single DataFrame if successful, or None if there were no valid DataFrames.
        """
        valid_dataframes = [df for df in dataframes if df is not None]

        if not valid_dataframes:
            self.logging.error("No valid DataFrames to concatenate.")
            return None

        if len(valid_dataframes) == 1:
            self.logging.info("Only one DataFrame provided, returning it without concatenation.")
            return valid_dataframes[0]

        try:
            combined_dataframe = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), valid_dataframes)
            self.logging.info(f"Successfully concatenated {len(valid_dataframes)} DataFrames with a total of {combined_dataframe.count()} rows.")
            return combined_dataframe
        except Exception as e:
            self.logging.error(f"Failed to concatenate DataFrames: {e}")
            return None

    def read_parquet_files(self, folder_path: str) -> Optional[DataFrame]:
        """
        Reads all Parquet files in the specified folder and concatenates them into a single DataFrame.
        
        Args:
        - folder_path (str): Path to the folder containing the Parquet files.
        
        Returns:
        - Optional[DataFrame]: The concatenated DataFrame if successful, or None if no valid files were found.
        """
        if not os.path.isdir(folder_path):
            self.logging.error(f"Folder path does not exist: {folder_path}")
            return None
        
        parquet_files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]
        
        if not parquet_files:
            self.logging.error("No Parquet files found in the directory.")
            return None

        self.logging.info(f"Found {len(parquet_files)} Parquet files in {folder_path}.")
        
        dataframes = [self.read_parquet_file(folder_path, filename) for filename in parquet_files]

        return self.concatenate(dataframes)

    def generate_deterministic_uuid_udf(*columns):
        """
        Creates a UDF that generates a deterministic UUID based on the values of the specified columns.

        Args:
            *columns: Variable length argument list representing column names.

        Returns:
            A UDF that can be applied to a Spark DataFrame to generate UUIDs.
        """
        def generate_deterministic_uuid(*args):
            # Concatenate the column values (attributes) to form a unique string
            combined_attributes = "".join(str(arg) for arg in args if arg is not None)
            # Generate a UUID based on a fixed namespace using the combined attributes
            return str(uuid.uuid5(uuid.NAMESPACE_DNS, combined_attributes))

        # Return the UDF
        return udf(generate_deterministic_uuid, StringType())
