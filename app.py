from pyspark.sql import SparkSession

######################################################################################
######################################## EDA ######################################### 
######################################################################################

#####################################################################
# 1- Create a DataFrame using the provided dataset:
#####################################################################

# Initialize SparkSession
spark = SparkSession.builder.appName("DataAnalysis").getOrCreate()
#spark = SparkSession.builder.master("local").appName("DataAnalysis").getOrCreate()

# Load the CSV data into a DataFrame
df = spark.read.option("header", "true").csv("dataset/eda.csv")

# Stop the SparkSession and close any associated resources
spark.stop()

#####################################################################
# 2- Create a DataFrame using the provided dataset:
#####################################################################

# Register DataFrame as a temporary view to use SQL queries
df.createOrReplaceTempView("customers")

# SQL query to find unique Customer IP count who lives in France
result_ip_count_france = spark.sql("""
    SELECT COUNT(DISTINCT customer_ip) AS UniqueIPCount
    FROM customers
    WHERE Country = 'France'
""")

# Collect the result and print it
unique_ip_count_france = result_ip_count_france.collect()[0]['UniqueIPCount']
print("Unique Customer IP count in France:", unique_ip_count_france)

#####################################################################
# 3- Average “ads_price” value between 1st of January 2023 and 3rd of March 2023 date interval
#####################################################################

from pyspark.sql.functions import col
from pyspark.sql.types import DateType

# Convert the 'date' column to DateType
df = df.withColumn("date", col("date").cast(DateType()))

# Filter the DataFrame to get data within the specified date interval
start_date = "2023-01-01"
end_date = "2023-03-03"
filtered_df = df.filter((col("date") >= start_date) & (col("date") <= end_date))

print(filtered_df.show(5))

# Calculate the average ads_price within the date interval
result_avg_ads_price = filtered_df.agg({"ads_price": "avg"}).collect()[0][0]
print("Average ads_price between 1st January 2023 and 3rd March 2023:", result_avg_ads_price)

#####################################################################
# 4- Find the country that has the maximum number of customers in 2022:
#####################################################################
"""
from pyspark.sql.functions import year

# Filter the DataFrame to get data for the year 2022
df_2022 = df.filter(year("date") == 2022)

# Group by 'country' and count the number of customers in each country
country_customers_count = df_2022.groupBy("country").count()

print(country_customers_count)

# Find the country with the maximum number of customers
#result_max_customers_country = country_customers_count.orderBy("count", ascending=False).first()['country']
#print("Country with the maximum number of customers in 2022:", result_max_customers_country)
"""

