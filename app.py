#####################################################################
# 1- Create a PySpark DataFrame which includes: 
# 'customer_id','age','ads_click_count','ads_price','country' and 'gender' features.
#####################################################################
from pyspark.sql import SparkSession

def read_data(path):
    spark = SparkSession.builder.master("local").appName("DataAnalysis").getOrCreate()

    # Load the CSV data into a DataFrame
    df = spark.read.option("header", "true").csv(path)

    # Select only the specified columns
    selected_columns = ['customer_id', 'age', 'ads_click_count', 'ads_price', 'country', 'gender']
    df = df.select(*selected_columns)
    return df

#####################################################################
# 2- Apply suitable missing value imputation techniques on the features 
# which have missing samples using with PySpark API.
#####################################################################
from pyspark.ml.feature import Imputer
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

def df_imputer(df,numeric_columns,categorical_columns):
    for column in numeric_columns:
        df = df.withColumn(column, col(column).cast(IntegerType()))

    # Impute missing values in numeric columns with mean
    imputer = Imputer(strategy='mean', inputCols=numeric_columns, outputCols=[col + '_imputed' for col in numeric_columns])
    df_imputed = imputer.fit(df).transform(df)

    # Iterate through the columns with missing values
    for column in categorical_columns:
        # Find the most frequent value for the current column
        most_frequent_value = df_imputed.groupBy(column).count().orderBy(col("count").desc()).first()[column]
        
        # Fill missing values in the current column with the most frequent value
        df_imputed = df_imputed.fillna(most_frequent_value, subset=[column])
    return df_imputed

#####################################################################
# 3- Apply a suitable scaling or standardization method for 
# numerical features which are 'ads_click_count' and 'ads_price' using with PySpark API.
#####################################################################
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler

def df_scaler(df,columns):
    # Initialize the StandardScaler
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)

    # Assemble the features into a single vector
    assembler = VectorAssembler(inputCols=columns, outputCol="features")
    df = assembler.transform(df)

    # Scale the features
    scaler_model = scaler.fit(df)
    df_scaled = scaler_model.transform(df)

    return df_scaled

#####################################################################
# 4- Apply a suitable encoding method for categorical features 
# which are “gender” and “country” using with PySpark API.
#####################################################################
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import explode

def df_encoder(df,columns):
    # Apply StringIndexer and OneHotEncoder on categorical columns using a pipeline
    indexers = [StringIndexer(inputCol=column, outputCol=column + "_index") for column in columns]
    encoder = [OneHotEncoder(inputCols=[column + "_index"], outputCols=[column + "_encoded"]) for column in columns]

    # Create a pipeline to chain the stages together
    pipeline_stages = indexers + encoder
    pipeline = Pipeline(stages=pipeline_stages)

    # Fit and transform the pipeline
    pipeline_model = pipeline.fit(df)
    df_encoded = pipeline_model.transform(df)
    df_encoded = df_encoded.drop("features")

    return df_encoded

#####################################################################
# TASK 3
#####################################################################
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

def cluester_generator(df, columns,k = 3):
    # Select the necessary columns for clustering from the DataFrame
    df_for_clustering = df.select(*columns)

    # Create a vector column from the scaled_features column
    vector_assembler = VectorAssembler(inputCols=['gender_encoded', 'country_encoded', 'scaled_features'], outputCol="features")
    df_for_clustering = vector_assembler.transform(df_for_clustering)

    # Initialize the K-Means model with the defined the number of clusters (k)
    kmeans = KMeans().setK(k).setSeed(1)

    # Fit the K-Means model to the data
    kmeans_model = kmeans.fit(df_for_clustering)

    # Make predictions using the K-Means model
    clustered_data = kmeans_model.transform(df_for_clustering)

    return clustered_data