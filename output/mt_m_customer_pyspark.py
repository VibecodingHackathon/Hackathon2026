# IDMC Mapping: Mapping0 (mt_m_customer)
# Generated: 2026-05-14
# Source: customers-100.csv (Flat File)
# Target: customers-100.csv (Flat File)
# Description: Customer master data mapping with company code lookup and derived fields

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("mt_m_customer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

logger.info("Spark session initialized for Mapping: Mapping0")

try:
    # ===== STEP 1: Define Source Schema =====
    # Schema for customers-100.csv source file
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
        StructField("Website", StringType(), True)
    ])
    
    # ===== STEP 2: Load Source Data =====
    # Read customers-100.csv from source location
    source_df = spark.read \
        .schema(source_schema) \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("encoding", "UTF-8") \
        .option("nullValue", "*") \
        .csv("C:/Users/hackathon12/Documents/IDMC/customers-100.csv")
    
    logger.info(f"Source data loaded. Row count: {source_df.count()}")
    
    # ===== STEP 3: Define Lookup Schema =====
    # Schema for COMPANY_CODE.csv lookup file
    lookup_schema = StructType([
        StructField("Company", StringType(), True),
        StructField("COMPANY_CODE", StringType(), True)
    ])
    
    # Load lookup data
    lookup_df = spark.read \
        .schema(lookup_schema) \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("encoding", "UTF-8") \
        .csv("C:/Users/hackathon12/Documents/IDMC/COMPANY_CODE.csv")
    
    logger.info(f"Lookup data loaded. Row count: {lookup_df.count()}")
    
    # ===== STEP 4: Apply Source Qualifier Transformations =====
    # Pass-through transformation: All fields are passed as-is from source
    qualified_df = source_df.select(
        col("Index"),
        col("Customer_Id"),
        col("First_Name"),
        col("Last_Name"),
        col("Company"),
        col("City"),
        col("Country"),
        col("Phone_1"),
        col("Phone_2"),
        col("Email"),
        col("Subscription_Date"),
        col("Website")
    )
    
    # ===== STEP 5: Perform Lookup Join =====
    # Lookup condition: Company = I_COMPANY
    # Join qualified_df with lookup_df on Company field
    lookup_condition = qualified_df.Company == lookup_df.Company
    
    joined_df = qualified_df.join(
        lookup_df,
        lookup_condition,
        "left"  # Left outer join to handle unmatched companies
    ).select(
        qualified_df.Index,
        qualified_df.Customer_Id,
        qualified_df.First_Name,
        qualified_df.Last_Name,
        qualified_df.Company,
        qualified_df.City,
        qualified_df.Country,
        qualified_df.Phone_1,
        qualified_df.Phone_2,
        qualified_df.Email,
        qualified_df.Subscription_Date,
        qualified_df.Website,
        lookup_df.COMPANY_CODE
    )
    
    logger.info("Lookup join completed")
    
    # ===== STEP 6: Apply Expression Transformations =====
    # Transformation 1: FULL_NAME = UPPER(First_Name) || UPPER(Last_Name)
    # Transformation 2: CRTN_ID = 'DEVELOPER'
    # Transformation 3: CRTN_DT_TM = SYSDATE (current timestamp)
    # Transformation 4: CMPNY_CD = LKP.LKP_COMPANY_CODE(Company) - Already from lookup
    
    expression_df = joined_df.select(
        col("Index"),
        col("Customer_Id"),
        col("First_Name"),
        col("Last_Name"),
        col("Company"),
        col("City"),
        col("Country"),
        col("Phone_1"),
        col("Phone_2"),
        col("Email"),
        col("Subscription_Date"),
        col("Website"),
        # Derived field: FULL_NAME = concatenate uppercase first and last names
        concat(upper(col("First_Name")), upper(col("Last_Name"))).alias("FULL_NAME"),
        # Derived field: CRTN_ID = static value 'DEVELOPER'
        lit("DEVELOPER").alias("CRTN_ID"),
        # Derived field: CRTN_DT_TM = current timestamp
        current_timestamp().alias("CRTN_DT_TM"),
        # Derived field: CMPNY_CD = value from lookup (nullable in case of no match)
        col("COMPANY_CODE").alias("CMPNY_CD")
    )
    
    logger.info("Expression transformations applied")
    
    # ===== STEP 7: Apply Update Strategy =====
    # Update Strategy: DD_INSERT (insert all rows)
    # Add IUD flag column for insert operation
    final_df = expression_df.select(
        col("Index"),
        col("Customer_Id"),
        col("First_Name"),
        col("Last_Name"),
        col("Company"),
        col("City"),
        col("Country"),
        col("Phone_1"),
        col("Phone_2"),
        col("Email"),
        col("Subscription_Date"),
        col("Website"),
        col("FULL_NAME"),
        col("CRTN_ID"),
        col("CRTN_DT_TM"),
        col("CMPNY_CD")
    )
    
    logger.info("Update strategy applied - Insert operation")
    
    # ===== STEP 8: Define Target Schema =====
    # Target file schema matches source with added derived fields
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
        StructField("FULL_NAME", StringType(), True),
        StructField("CRTN_ID", StringType(), True),
        StructField("CRTN_DT_TM", StringType(), True),
        StructField("CMPNY_CD", StringType(), True)
    ])
    
    # ===== STEP 9: Write Target Data =====
    # Write to target CSV file
    output_path = "C:/Users/hackathon12/Downloads/tgt/customers-100.csv"
    
    final_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("encoding", "UTF-8") \
        .csv(output_path)
    
    logger.info(f"Data written to {output_path}")
    logger.info(f"Total rows processed: {final_df.count()}")
    
    # ===== STEP 10: Display Sample Results =====
    logger.info("Sample of transformed data:")
    final_df.show(5, truncate=False)
    
    logger.info("Mapping execution completed successfully!")
    
except Exception as e:
    logger.error(f"Error during mapping execution: {str(e)}", exc_info=True)
    raise
    
finally:
    # Cleanup
    spark.stop()
    logger.info("Spark session closed")
