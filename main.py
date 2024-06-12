# Import necessary modules and functions
from resources.dev import config
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.read.aws_read import S3Reader
from src.main.download.aws_file_download import S3FileDownloader
from src.main.utility.spark_session import spark_session
from src.main.move import move_files
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.write.parquet_writer import ParquetWriter
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.delete.local_file_delete import delete_local_file
from src.main.move.move_files import move_s3_to_s3
import shutil
import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Retrieve AWS access keys from config
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key


# Create a S3 client object using decrypted AWS access keys
s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()
response = s3_client.list_buckets()
logger.info("List of buckets: %s", response['Buckets'])


# If present, check if the same file is present in the staging area
# with a status of 'A'. If so, do not delete the file and try to re-run.
# Otherwise, throw an error and do not proceed further.

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(f"'{file}'")
    
    statement = f"""
                SELECT DISTINCT file_name FROM
                {config.database_name}.{config.product_staging_table}
                WHERE file_name IN ({', '.join(total_csv_files)}) AND status = 'A';
                """
    logger.info(f"Dynamically created statement: {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info("Your last run failed. Please check the status of the file in the staging area.")
    else:
        logger.info("No records matched.")
else:
    logger.info("Last run was successful!")

try:
    s3_reader = S3Reader()
    # Bucket name should be read from the configuration.
    folder_path = config.s3_source_directory
    s3_absolute_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path)
    logger.info("Absolute path of the files: %s", s3_absolute_path)
    if not s3_absolute_path:
        logger.info(f"No files found in the folder: {folder_path}")
        raise Exception(f"No data available to process in the folder: {folder_path}")

except Exception as e:
    logger.error("Exited with error: %s", e)
    raise e

# Load bucket name and local directory configuration
bucket_name = config.bucket_name
local_directory = config.local_directory

# Construct the S3 prefix for the bucket
prefix = f"s3://{bucket_name}/"

# Extract file paths relative to the S3 bucket from the absolute S3 paths
file_paths = [url[len(prefix):] for url in s3_absolute_path]

# Log the bucket name and the file paths that will be downloaded
logger.info("File path available on s3 bucket under name %s and path %s", bucket_name, file_paths)

try:
    # Initialize the S3 file downloader
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    
    # Download the files from S3 to the local directory
    downloader.download_files(file_paths)
except Exception as e:
    # Log any error that occurs during the download process and exit the program
    logger.error("Error in downloading files: %s", e)
    sys.exit()

# Get a list of all the files in the local directory after the download
all_files = os.listdir(local_directory)
logger.info(f"List of files present in the local directory after download: {all_files}")

# Filter the files to find CSV files and create their absolute paths
if all_files:
    csv_files = []
    error_files = []
    
    # Iterate over all files to separate CSV files and non-CSV files
    for file in all_files:
        if file.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, file)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, file)))
    
    # If no CSV files are found, log an error and raise an exception
    if not csv_files:
        logger.error("No CSV data available in the local directory.")
        raise Exception("No CSV data available in the local directory.")
else:
    # If no files are present in the local directory, log an error and raise an exception
    logger.error("There is no data to process.")
    raise Exception("There is no data to process.")

# Convert the CSV files string representation into a list
# csv_files = str(csv_files)[1:-1]
logger.info("*****************List of CSV files*****************")
logger.info("List of CSV files that needs to be processed %s", csv_files)

# Initialize and create a Spark session
logger.info("*****************Creating a spark session*****************")
spark = spark_session()
logger.info("*****************Spark session created.*****************")

# Check the required columns in the schema of CSV files
# If there are any columns that are not required, keep them in a list or error_files
# Else, union all the data into one dataframe

logger.info("*****************Checking the schema of the CSV files loaded in S3*****************")

# List to store the files with correct schemas
correct_files = []

# Iterate through each CSV file to check its schema
for data in csv_files:
    # Read the CSV file to get its schema
    data_schema = spark.read.format("csv")\
        .option("header", "true")\
        .load(data).columns
    
    # Log the schema of the current file
    logger.info(f"Schema of the file {data} is: {data_schema}")
    logger.info(f"Required columns are: {config.mandatory_columns}")
    
    # Determine any missing required columns
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"Missing columns in the file {data} are: {missing_columns}")

    # If there are missing columns, add the file to error_files
    if missing_columns:
        error_files.append(data)
    else:
        # If all required columns are present, add the file to correct_files
        logger.info(f"File {data} has all the required columns.")
        correct_files.append(data)

# Log the files with correct schemas
logger.info(f"*****************Correct files*****************- {correct_files}")

# If there are any files with missing columns, log them and handle accordingly
if error_files:
    logger.info(f"*****************Error files*****************- {error_files}")
    logger.info("Moving the error files to the error folder.")
else:
    logger.info("No error files found. Proceeding further.")

# Move the error files to the error folder locally
error_folder_local_path = config.error_folder_path_local
if error_files:
    for file in error_files:
        # Check if the error folder exists
        if os.path.exists(error_folder_local_path):
            # Determine the file name and destination path
            file_name = os.path.basename(file)
            destination_path = os.path.join(error_folder_local_path, file_name)
            
            # Move the error file to the local error folder
            shutil.move(file, destination_path)
            logger.info(f"Error file {file} moved from S3 Downloads to the {destination_path} folder.")
            
            # Move the file in S3 from source directory to error directory
            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory
            message = move_files(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
            logger.info(f"{message}")
        else:
            # Log an error if the error folder does not exist
            logger.error(f"File {file} not moved to the error folder as the folder does not exist.")
else:
    logger.info("*****************No error files found. Proceeding further.*****************")

# Additional columns need to be taken care of
# Determining extra columns
# Before running the process,
# Stage table needs to be updated with the file name and status as 'I' or 'A'
logger.info("*****************Updating the staging table*****************")
insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
    for files in correct_files:
        # Determine the file name
        filename = os.path.basename(files)
        
        # Create the insert statement for the staging table
        statement = f"""INSERT INTO {db_name}.{config.product_staging_table}
        (file_name, file_location, created_date, status)
        VALUES ('{filename}', '{filename}' , '{current_date}' , 'A');"""
        insert_statements.append(statement)
        logger.info(f"Insert statement created: {statement}")
        
        # Connect to MySQL server
        logger.info("*****************Connecting to MySQL Server*****************")
        connection = get_mysql_connection()
        cursor = connection.cursor()
        logger.info("*****************Connected to MySQL Server*****************")
        
        # Execute the insert statements
        for statement in insert_statements:
            cursor.execute(statement)
            connection.commit()
            logger.info(f"Data inserted into the table {config.product_staging_table} successfully.")
        
        # Close the cursor and connection
        cursor.close()
        connection.close()
else:
    # Log an error and raise an exception if no files are found to process
    logger.error("No files to process. Exiting the process.")
    raise Exception("No files to process. Exiting the process.")

logger.info("***************** Staging table updated successfully. *****************")
logger.info("***************** Fixing extra columns coming from source. *****************")

# Define schema for the DataFrame
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True), 
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True)
])

logger.info("*****************Creating empty dataframe.*****************")

final_df_to_process = spark.createDataFrame([], schema)
final_df_to_process.show()
# Process each file in correct_files
for data in correct_files:
    # Read the CSV file into a DataFrame
    data_df = spark.read.format("csv")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load(data)
    # Get the columns of the current DataFrame
    data_schema = data_df.columns
    # Identify extra columns that are not in the mandatory columns list
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    # Log the extra columns found in the current file
    logger.info(f"Extra columns in the file {data} at source are: {extra_columns}")
    # Check if there are any extra columns
    if extra_columns:
        # Concatenate the values of extra columns into a single string and add it as 'additional_column'
        data_df = data_df.withColumn("additional_column", concat_ws(", ", *extra_columns))\
        .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost", "additional_column")
        # Log the processing of the current file
        logger.info(f"Processed {data} and added a new column with concatenated values of extra columns.")
    else:
        # If no extra columns, add 'additional_column' with None values
        data_df = data_df.withColumn("additional_column", lit(None))\
        .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity", "total_cost", "additional_column")
        # Log the processing of the current file
        logger.info(f"Processed {data} and added a new column with None values.")

    # Append the processed DataFrame to the final DataFrame
    final_df_to_process = final_df_to_process.union(data_df)

# Log the final DataFrame that will be processed
logger.info("***************** Final dataframe from source which will be processed: *****************")
final_df_to_process.show()

# Enrich the data from all dimension tables
# Also create a datamart for the sales team including their incentives, addresses, and more.
# Another datamart for customers indicating how many products they bought on each day of the month.
# For every month, generate a file with store_id segregation.
# Read the data from Parquet and generate a CSV file.
# The CSV will include: sales_person_name, sales_person_store_id, 
# sales_person_total_billing_done_for_the_month, total_incentive.

# Connect with DatabaseReader
database_client = DatabaseReader(config.url, config.properties)

# Create DataFrames for all tables
# Customer Table
logger.info("Loading the customer table into a customer_table_df.")
customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)

# Product Table
logger.info("Loading the product table into a product_table_df.")
product_table_df = database_client.create_dataframe(spark, config.product_table)

# Product Staging Table
logger.info("Loading the product staging table into a product_staging_table_df.")
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)

# Sales Team Table
logger.info("Loading the sales team table into a sales_team_table_df.")
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)

# Store Table
logger.info("Loading the store table into a store_table_df.")
store_table_df = database_client.create_dataframe(spark, config.store_table)

# Joining dimension tables
s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process, customer_table_df, store_table_df, sales_team_table_df)

logger.info("*****************Final enriched info*****************")
s3_customer_store_sales_df_join.show()

#Write the customers data into customer_data_mart
#file will be written to local first
#Move the RAW data to S3 bucket for reporting tool
#Write reporting data into SQL table also
logger.info("*****************Writing the data into the final_customer_data_mart_df*****************")
final_customer_data_mart_df = s3_customer_store_sales_df_join\
    .select("ct.customer_id",
            "ct.first_name",
            "ct.last_name",
            "ct.address",
            "ct.pincode",
            "phone_number",
            "sales_date",
            "total_cost")
logger.info("*****************Final data customer data mart*****************")
final_customer_data_mart_df.show()

parquet_writer = ParquetWriter("overwrite", "parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df, config.customer_data_mart_local_file)

logger.info(f"*****************Data written to the local file at {config.customer_data_mart_local_file}*****************")

#Move the data to S3 bucket
logger.info("*****************Moving the data to S3 bucket*****************")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.customer_data_mart_local_file)
logger.info(f"{message}")

#Sales team data mart
logger.info("*****************Write data into sales team data mart*****************")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
                                .select("store_id",
                                        "sales_person_id",
                                        "sales_person_first_name",
                                        "sales_person_last_name",
                                        "store_manager_name",
                                        "manager_id",
                                        "is_manager",
                                        "sales_person_address",
                                        "sales_person_pincode",
                                        "sales_date",
                                        "total_cost",
                                        expr("SUBSTRING(sales_date, 1, 7) as sales_month"))

logger.info("*****************Final data sales team data mart*****************")
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df, config.sales_team_data_mart_local_file)
logger.info(f"*****************sales team data written to the local file at {config.sales_team_data_mart_local_file}*****************")

s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.sales_team_data_mart_local_file)
logger.info(f"{message}")

#Also writing the data info partitioned data
final_sales_team_data_mart_df.write.format("parquet")\
                            .option("header", "true")\
                            .partitionBy("sales_month", "store_id")\
                            .mode("overwrite")\
                            .option("path", config.sales_team_data_mart_partitioned_local_file)\
                            .save()

s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root, file)
        relative_file_path = os.path.relpath(local_file_path, config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

#Calculation for data mart
#Find out the customers total purchases in a month
#Write the result into MySQL table
logger.info("Calculating the total purchases of customers in a month.")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("Calculation done and written to the MySQL table.")

# Calculation for sales team mart

# Calculate the total sales done by each sales person in a month
logger.info("Calculating the total sales done by each sales person in a month.")

# Write the result into a MySQL table
# The top-performing sales person of the month will receive a 1% incentive
# The rest of the sales team members receive no incentive
sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info("Calculation done and written to the MySQL table.")

# Move the processed files to the 'processed' folder in the S3 bucket
source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix)
logger.info(f"{message}")

# Delete downloaded files from the local directory
logger.info("*****************Deleting downloaded files from local.*****************")
delete_local_file(config.local_directory)
logger.info("*****************Files deleted from local.*****************")

# Delete customer_data_mart_local_file from the local directory
logger.info("*****************Deleting customer_data_mart_local_file from local*****************")
delete_local_file(config.customer_data_mart_local_file)
logger.info("*****************Deleted customer_data_mart_local_file from local*****************")

# Delete sales team data mart file from the local directory
logger.info("*****************Deleting sales team data from local*****************")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("*****************Deleted sales team data from local*****************")

# Delete sales team partitioned data file from the local directory
logger.info("*****************Deleting sales team partitioned data from local*****************")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("*****************Deleted sales team partitioned data from local*****************")


# Update the status of the staging table
update_statements = []

# If there are correct files to process
if correct_files:
    # Create update statements for each correct file
    for file in correct_files:
        filename = os.path.basename(file)
        statement = f"""
                    UPDATE {db_name}.{config.product_staging_table}
                    SET status = 'I', updated_date = '{current_date}'
                    WHERE file_name = '{filename}';
                    """
        update_statements.append(statement)
    
    # Log the created update statements
    logger.info(f"Update statements created: {update_statements}")
    
    # Connect to the MySQL Server
    logger.info("*****************Connecting to MySQL Server*****************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("*****************Connected to MySQL Server*****************")
    
    # Execute each update statement and commit the changes
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
        logger.info(f"Data updated in the table {config.product_staging_table} successfully.")
    
    # Close the cursor and connection
    cursor.close()
    connection.close()
else:
    # Log an error if there are no correct files to process
    logger.error("There has been an error in updating the status of the staging table.")
    sys.exit()

# Wait for user input to exit
input("Press Enter to exit...")