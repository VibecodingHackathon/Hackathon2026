"""
===============================================================================
IDMC to PySpark Conversion: Customer Mapping
===============================================================================
Mapping Name:     mt_m_customer
Source:           customers-100.csv (Flat File)
Target:           customers-100.csv (Flat File)
Created Date:     2026-05-13
Description:      Converts customer data with name concatenation and company
                  code lookup transformation
===============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)
from pyspark.sql.functions import (
    col, upper, concat, lit, current_timestamp, trim, coalesce
)
import logging
from datetime import datetime

# ===============================================================================
# Configuration
# ===============================================================================

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# File paths (configure these based on your environment)
SOURCE_FILE_PATH = "C:\\Users\\hackathon12\\Documents\\IDMC\\customers-100.csv"
LOOKUP_FILE_PATH = "C:\\Users\\hackathon12\\Documents\\IDMC\\COMPANY_CODE.csv"
TARGET_FILE_PATH = "C:\\Users\\hackathon12\\Downloads\\tgt\\customers-100.csv"

# ===============================================================================
# PySpark Session Initialization
# ===============================================================================

def create_spark_session():
    """Initialize and return Spark session for customer mapping."""
    spark = SparkSession.builder \
        .appName("mt_m_customer_mapping") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
    
    logger.info("Spark session created successfully")
    return spark


# ===============================================================================
# Source Schema Definition
# ===============================================================================

def get_source_schema():
    """
    Define the source data schema for customers-100.csv
    All fields are string type (nstring in IDMC)
    """
    source_schema = StructType([
        StructField("Index", StringType(), True),
        StructField("Customer_Id", StringType(), True),
        StructField("First_Name", StringType(), True),
        StructField("Last_Name", StringType(), True),
        StructField("Company", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Phone_1", StringType(), True),
        StructField("Phone_2", StringType(), True),
        StructField("Email", StringType(), True),
        StructField("Subscription_Date", StringType(), True),
        StructField("Website", StringType(), True),
    ])
    
    return source_schema


def get_target_schema():
    """
    Define the target data schema for customers-100.csv
    Includes original fields plus derived fields
    """
    target_schema = StructType([
        StructField("Index", StringType(), True),
        StructField("Customer_Id", StringType(), True),
        StructField("First_Name", StringType(), True),
        StructField("Last_Name", StringType(), True),
        StructField("Company", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Phone_1", StringType(), True),
        StructField("Phone_2", StringType(), True),
        StructField("Email", StringType(), True),
        StructField("Subscription_Date", StringType(), True),
        StructField("Website", StringType(), True),
        StructField("FULL_NAME", StringType(), True),         # Derived field
        StructField("CRTN_ID", StringType(), True),          # Derived field
        StructField("CRTN_DT_TM", TimestampType(), True),    # Derived field
        StructField("CMPNY_CD", StringType(), True),         # Derived field (lookup)
    ])
    
    return target_schema


# ===============================================================================
# Data Loading Functions
# ===============================================================================

def load_source_data(spark, source_path):
    """
    Load source customer data from CSV file
    
    Args:
        spark: SparkSession object
        source_path: Path to source CSV file
        
    Returns:
        DataFrame with source data
    """
    try:
        logger.info(f"Loading source data from {source_path}")
        
        source_df = spark.read \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("nullValue", "*") \
            .option("inferSchema", "false") \
            .schema(get_source_schema()) \
            .csv(source_path)
        
        row_count = source_df.count()
        logger.info(f"Successfully loaded {row_count} rows from source")
        
        return source_df
        
    except Exception as e:
        logger.error(f"Error loading source data: {str(e)}")
        raise


def load_lookup_data(spark, lookup_path):
    """
    Load company code lookup data from CSV file
    
    Args:
        spark: SparkSession object
        lookup_path: Path to lookup CSV file
        
    Returns:
        DataFrame with lookup data
    """
    try:
        logger.info(f"Loading lookup data from {lookup_path}")
        
        lookup_df = spark.read \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("nullValue", "*") \
            .option("inferSchema", "false") \
            .csv(lookup_path)
        
        # Cache lookup table for repeated joins
        lookup_df.cache()
        row_count = lookup_df.count()
        logger.info(f"Successfully loaded {row_count} rows from lookup table")
        
        return lookup_df
        
    except Exception as e:
        logger.error(f"Error loading lookup data: {str(e)}")
        raise


# ===============================================================================
# Transformation Logic
# ===============================================================================

def transform_customer_data(spark, source_df, lookup_df):
    """
    Apply all transformations from the IDMC mapping:
    
    1. Pass-through fields: Index, Customer_Id, First_Name, Last_Name, etc.
    2. FULL_NAME = UPPER(First_Name) || UPPER(Last_Name)
       Expression: Concatenate uppercase first and last names
    
    3. CRTN_ID = 'DEVELOPER'
       Expression: Static literal value
    
    4. CRTN_DT_TM = SYSDATE (current timestamp)
       Expression: Current system date/time
    
    5. CMPNY_CD = Company Code lookup
       Expression: Lookup Company code from lookup table based on Company field
       LookupCondition: Company = I_COMPANY
    
    Args:
        spark: SparkSession object
        source_df: Source DataFrame
        lookup_df: Lookup DataFrame for company codes
        
    Returns:
        Transformed DataFrame ready for target
    """
    try:
        logger.info("Starting data transformations")
        
        # Step 1: Perform lookup join for company codes
        # Join source data with lookup table on Company field
        logger.info("Performing lookup join for COMPANY_CODE")
        
        # Left join to preserve all source records even if no match found
        joined_df = source_df.join(
            lookup_df,
            source_df["Company"] == lookup_df["Company"],
            "left"
        )
        
        # Step 2: Apply all transformations in a single select
        transformed_df = joined_df.select(
            # Pass-through fields (unchanged)
            col("Index").alias("Index"),
            col("Customer_Id").alias("Customer_Id"),
            col("First_Name").alias("First_Name"),
            col("Last_Name").alias("Last_Name"),
            col("Company").alias("Company"),
            col("City").alias("City"),
            col("Country").alias("Country"),
            col("Phone_1").alias("Phone_1"),
            col("Phone_2").alias("Phone_2"),
            col("Email").alias("Email"),
            col("Subscription_Date").alias("Subscription_Date"),
            col("Website").alias("Website"),
            
            # FULL_NAME: Concatenate UPPER(First_Name) and UPPER(Last_Name)
            # IDMC Expression: UPPER(First_Name)||UPPER(Last_Name)
            # PySpark equivalent: concat uppercase columns
            concat(
                upper(col("First_Name")),
                upper(col("Last_Name"))
            ).alias("FULL_NAME"),
            
            # CRTN_ID: Static value 'DEVELOPER'
            # IDMC Expression: 'DEVELOPER'
            lit("DEVELOPER").alias("CRTN_ID"),
            
            # CRTN_DT_TM: Current timestamp
            # IDMC Expression: SYSDATE
            # PySpark equivalent: current_timestamp()
            current_timestamp().alias("CRTN_DT_TM"),
            
            # CMPNY_CD: Lookup result
            # IDMC Expression: :LKP.LKP_COMPANY_CODE(Company)
            # PySpark equivalent: column from lookup join
            coalesce(
                col("COMPANY_CODE"),  # Will be null if no match
                lit("UNKNOWN")        # Default value if no match
            ).alias("CMPNY_CD")
        )
        
        logger.info("Data transformations completed successfully")
        
        return transformed_df
        
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        raise


# ===============================================================================
# Data Validation Functions
# ===============================================================================

def validate_transformed_data(df):
    """
    Validate transformed data quality
    
    Args:
        df: Transformed DataFrame
    """
    try:
        logger.info("Starting data validation")
        
        # Check row count
        row_count = df.count()
        logger.info(f"Total rows after transformation: {row_count}")
        
        # Check for null values in critical fields
        null_checks = {
            "Customer_Id": df.filter(col("Customer_Id").isNull()).count(),
            "FULL_NAME": df.filter(col("FULL_NAME").isNull()).count(),
            "CRTN_ID": df.filter(col("CRTN_ID").isNull()).count(),
            "CRTN_DT_TM": df.filter(col("CRTN_DT_TM").isNull()).count(),
        }
        
        for field, null_count in null_checks.items():
            if null_count > 0:
                logger.warning(f"Field '{field}' has {null_count} null values")
        
        # Display sample records
        logger.info("Sample transformed records:")
        df.show(5, truncate=False)
        
        # Print schema
        logger.info("Target schema:")
        df.printSchema()
        
    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        raise


# ===============================================================================
# Data Write Functions
# ===============================================================================

def write_target_data(df, target_path):
    """
    Write transformed data to target CSV file
    
    Args:
        df: DataFrame to write
        target_path: Path to target CSV file
    """
    try:
        logger.info(f"Writing data to target: {target_path}")
        
        # Write with header and overwrite mode
        df.coalesce(1).write \
            .option("header", "true") \
            .option("delimiter", ",") \
            .option("nullValue", "*") \
            .mode("overwrite") \
            .csv(target_path)
        
        logger.info(f"Successfully wrote data to {target_path}")
        
    except Exception as e:
        logger.error(f"Error writing target data: {str(e)}")
        raise


# ===============================================================================
# Main Execution Function
# ===============================================================================

def main():
    """
    Main orchestration function for customer mapping
    Coordinates all steps: load, transform, validate, and write
    """
    spark = None
    
    try:
        logger.info("=" * 80)
        logger.info("Starting IDMC Customer Mapping Conversion")
        logger.info("=" * 80)
        
        # Initialize Spark session
        spark = create_spark_session()
        
        # Load source data
        source_df = load_source_data(spark, SOURCE_FILE_PATH)
        
        # Load lookup data
        lookup_df = load_lookup_data(spark, LOOKUP_FILE_PATH)
        
        # Apply transformations
        transformed_df = transform_customer_data(spark, source_df, lookup_df)
        
        # Validate transformed data
        validate_transformed_data(transformed_df)
        
        # Write to target
        write_target_data(transformed_df, TARGET_FILE_PATH)
        
        logger.info("=" * 80)
        logger.info("IDMC Customer Mapping Conversion Completed Successfully")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"IDMC Customer Mapping Conversion Failed: {str(e)}")
        logger.error("=" * 80)
        raise
        
    finally:
        # Clean up
        if spark:
            logger.info("Stopping Spark session")
            spark.stop()


# ===============================================================================
# Script Entry Point
# ===============================================================================

if __name__ == "__main__":
    main()
