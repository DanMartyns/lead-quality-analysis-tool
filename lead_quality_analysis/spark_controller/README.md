# Spark Controller module
```bash
├── spark_controller/
│   ├── __init__.py           # Package initialization
│   ├── controller.py         # Utils script with useful functions to help the process
│   └── schemas.json          # JSON file with schema definitions
```

## Overview
This documentation outlines the structure and functionality of the provided PySpark-based data pipeline. It offers methods for reading files, cleaning data, and enforcing schemas, making it suitable for data engineering tasks that require robust data quality checks. The modular design allows for flexible and reusable operations on Spark DataFrames, and the logging functionality ensures that the pipeline is well-monitored.

## Structure
- **Logging Configuration**: Configures the logger to capture events for debugging and monitoring.
- **Classes**:
    - **Dataframe**: Encapsulates operations on Spark DataFrames, such as cleaning, schema enforcement, and validation.
    - **SparkController**: Manages Spark session creation and provides utility methods for reading CSV and Parquet files.

## Logging Configuration
```python
    logging.basicConfig(
        level=logging.INFO,  # Default logging level
        format='%(asctime)s - %(levelname)s - %(message)s',  # Log format includes timestamps
        handlers=[
            logging.StreamHandler(sys.stdout)  # Logs are sent to stdout
        ]
    )
```
This block sets up logging for the entire application, capturing logs at INFO level and higher. Logs are printed to stdout.

## `Dataframe` class

### `__init__(self, dataframe: DataFrame)`
This method initializes a new instance of the Dataframe class, accepting a Spark DataFrame. It also loads a schema definition from a JSON file for schema enforcement during the cleaning process.

Args:
- *dataframe*: The Spark Dataframe that we be affected by all the transformation functions offer by this class

### `__get_schema(self, schema_name: str) -> dict`
Retrieves a schema definition from a JSON file (`schemas.json`). If the schema is not found, it raises a ValueError.

Args:
- *schema_name*: The name of the schema to apply.

### `clean_dataframe(self) -> "Dataframe"`
Performs the following operations:
- Whitespace Trimming: Removes leading, trailing, and multiple spaces within string columns.
- Duplicate Removal: Removes duplicate rows.
- Null Row Removal: Drops rows where all columns contain NULL.

**Example Usage**:
```python
cleaned_df = Dataframe(df).clean_dataframe().to_dataframe()
```

### `clean_invalid_values(self, column: str, invalid_values: List[str]) -> "Dataframe"`
Replaces specified invalid values within a column with NULL.

**Args**:
- *column*: The column to clean.
- *invalid_values*: A list of invalid values to replace with NULL.

### `enforce_schema(self, schema_name: str, action: str = 'replace') -> "Dataframe"`
This method enforces a predefined schema on the DataFrame, handling two aspects:

- *Column Names*: Standardizes column names based on schema definition (e.g., converting to snake_case).
- *Data Types*: Casts columns to their expected types (e.g., date, integer, boolean, etc.).

**Args**:
- *schema_name*: The name of the schema to apply.
- *action*: Defines behavior for invalid values.
    - *replace*: Replaces invalid values with NULL.
    - *log*: Logs invalid values but keeps them in the DataFrame.

## `SparkController` class

### `__init__(self)`
Initializes a Spark session and configures a legacy parameter.

- Legacy Time Parsing: Enables legacy time parsing to handle datetime formats like MM/dd/yyyy hh:mmAM/PM.

### `read_csv_file(self, folder_path: str, filename: str, delimiter: str = ',', schema: Optional[StructType] = None, header: bool = True) -> Optional[DataFrame]`
Reads a CSV file into a Spark DataFrame, handling errors like file non-existence or incorrect format.

Args:
- *folder_path*: Path to the folder containing the CSV.
- *filename*: The name of the file.
- *delimiter*: Optional delimiter (default is a comma).
- *schema*: Optional predefined schema for the DataFrame.
- *header*: Whether the file has a header row (default is True).

### `concatenate(dataframes: List[Optional[DataFrame]]) -> Optional[DataFrame]`
This method concatenates a list of PySpark DataFrames using the unionByName function. It handles missing columns automatically.

Args:
- *dataframes*: A list of DataFrames to concatenate. DataFrames that are None are automatically filtered out.

Returns:
- A single concatenated DataFrame, or None if no valid DataFrames were provided.

Example:
```python
result_df = controller.concatenate([df1, df2, df3])
```

### `read_parquet_files(folder_path: str) -> Optional[DataFrame]`
This method reads all Parquet files in the specified folder and concatenates them into a single DataFrame.

Args:
- **folder_path**: The path to the folder containing the Parquet files.

Returns:
- A concatenated DataFrame if the operation is successful, or None if no valid Parquet files are found.

Example:
```python
result_df = controller.read_parquet_files("/path/to/parquet/folder")
```

### `generate_deterministic_uuid_udf(*columns) -> UDF`
This method generates a PySpark UDF (User Defined Function) that creates deterministic UUIDs based on the values of specified columns. It is useful when you need a unique identifier for each record that can be derived from column values.

Args:
    - *columns: One or more column names from which to generate the UUID.

Returns:
    - A PySpark UDF that can be applied to a DataFrame column.

Example:
```python
uuid_udf = util.generate_deterministic_uuid_udf('col1', 'col2')
df.withColumn("uuid", uuid_udf(df['col1'], df['col2']))
```

