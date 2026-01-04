from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta
import builtins
import os

def generate_sample_data(spark, num_records=10):
    products = [
        (
            f"PROD{i:04d}",
            f"Product_{i}",
            random.choice(['Electronics', 'Clothing', 'Home', 'Books', 'Sports']),
            builtins.round(random.uniform(10, 500), 2)
        )
        for i in range(1,1001)
    ]

    product_schema = StructType([
        StructField("ProductID", StringType(), False),
        StructField("ProductName", StringType(), False),
        StructField("Category", StringType(), False),
        StructField("UnitPrice", DoubleType(), False)
    ])

    products_df = spark.createDataFrame(products, schema=product_schema)

    customers = [
        (
            f"CUST{i:06d}",
            f"customer{i}@email.com",
            random.choice(['US', 'UK', 'CA', 'AU', 'IN'])
        )
        for i in range(1, 50001)
    ]

    customer_schema = StructType([
        StructField("CustomerID", StringType(), False),
        StructField("Email", StringType(), False),
        StructField("Country", StringType(), False)
    ])

    customers_df = spark.createDataFrame(customers, schema=customer_schema)

    print(f"Generating {num_records:,} transaction records...")

    transactions = []

    start_date = datetime.now() - timedelta(days=730)
    end_date = datetime.now()

    for i in range(num_records):
        invoice_no = f"INV{random.randint(100000, 999999)}"
        product_id = f"PROD{random.randint(1, 1000):04d}"
        customer_id = f"CUST{random.randint(1, 50000):06d}"
        quantity = random.choices([1, 2, 3, 4, 5, 10, 20, 50], weights=[40, 25, 15, 10, 5, 3, 1, 1],  k=1)[0]
        country = random.choice(['US', 'UK', 'CA', 'AU', 'IN'])
        
        random_seconds = random.randint(0, int((end_date - start_date).total_seconds()))
        invoice_date = start_date + timedelta(seconds=random_seconds)

        if random.random() < 0.05:
            if random.random() < 0.3:
                quantity = -quantity
            elif random.random() < 0.5:
                customer_id = None
            else:
                quantity = 0
        
        transactions.append((
            invoice_no,
            product_id,
            customer_id,
            quantity,
            invoice_date,
            country
        ))

        if (i + 1) % 100000 == 0:
            print(f"Generated {i + 1:,} records...")

        transactions_schema = StructType([
            StructField("InvoiceNo", StringType(), False),
            StructField("ProductID", StringType(), False),
            StructField("CustomerID", StringType(), True), 
            StructField("Quantity", IntegerType(), False),
            StructField("InvoiceDate", TimestampType(), False),
            StructField("Country", StringType(), False)
        ])
    
    print("Creating DataFrame from generated data...")
    transactions_df = spark.createDataFrame(transactions, schema=transactions_schema)

    transactions_df = transactions_df.join(
        products_df.select("ProductID", "UnitPrice"),
        on="ProductID",
        how="left"
    )

    transactions_df = transactions_df.withColumn(
        "TotalAmount",
        col("Quantity") * col("UnitPrice")
    )

    transactions_df = transactions_df.select(
        "InvoiceNo",
        "InvoiceDate",
        "ProductID",
        "CustomerID",
        "Quantity",
        "UnitPrice",
        "TotalAmount",
        "Country"
    )
    
    print(f"Data generation complete!")
    print(f"Total Transactions: {transactions_df.count():,}")
    print(f"Total Products: {products_df.count():,}")
    print(f"Total Customers: {customers_df.count():,}")
    
    return transactions_df, products_df, customers_df


if __name__ == "__main__":

    os.environ["PYSPARK_PYTHON"] = r"C:\Python38\python.exe"
    os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Python38\python.exe"

    spark = SparkSession.builder \
        .appName("Data Generator") \
        .config("spark.driver.memory", "4g") \
        .master("local[*]") \
        .getOrCreate()
    

    transactions_df, products_df, customers_df = generate_sample_data(
        spark, 
        num_records= 10 #1000000  # 1 million transactions
    )
    
    # Show sample data
    print("\n=== Sample Transactions ===")
    transactions_df.show(10, truncate=False)
    
    print("\n=== Sample Products ===")
    products_df.show(10, truncate=False)
    
    print("\n=== Sample Customers ===")
    customers_df.show(10, truncate=False)
    
    spark.stop()