# clustering-pyspark

# Data Processing Pipeline

This repository contains a data processing pipeline implemented in PySpark for handling data obtained from Mackaroo API, performing exploratory data analysis (EDA), and applying K-Means clustering algorithm on the preprocessed data. The pipeline consists of the following steps:

1. **req_data.py**: This script fetches data from the Mackaroo API using an API key and saves it to a local CSV file.

2. **eda.py**: This script performs exploratory data analysis (EDA) on the raw data. It reads the data from the CSV file, initializes a SparkSession, and performs various EDA steps to understand the dataset's characteristics.

3. **app.py**: This script contains the data preprocessing steps and the K-Means clustering algorithm. It includes functions for imputing missing values, scaling numerical features, encoding categorical features, and generating clusters using K-Means.

4. **main.py**: This script serves as the main entry point to the data processing pipeline. It imports the necessary functions from `app.py`, `req_data.py`, and `eda.py`, and orchestrates the entire pipeline.

## Steps to Run the Pipeline

1. Ensure you have Apache Spark installed and set up properly.

2. Obtain a Mackaroo API key by signing up on the Mackaroo website (https://www.mackaroo.com/).

3. Modify the `API_KEY` variable in `main.py` with your Mackaroo API key.

4. Run `main.py` to execute the entire data processing pipeline. This will fetch data from the Mackaroo API, perform EDA, preprocess the data, and apply the K-Means clustering algorithm.

## File Descriptions

1. `req_data.py`: Contains functions for fetching data from the Mackaroo API and saving it to a local CSV file.

2. `eda.py`: Contains functions for performing exploratory data analysis (EDA) on the raw data.

3. `app.py`: Contains functions for data preprocessing and K-Means clustering.

4. `main.py`: The main script that calls functions from `req_data.py`, `eda.py`, and `app.py` to execute the data processing pipeline.

5. `dataset/eda.csv`: The CSV file where data fetched from the Mackaroo API is stored.

## Dependencies

- PySpark: 3.1.2 or higher
- requests: 2.26.0 or higher

## Usage

1. Install the required dependencies by running:

pip install -r requirements.txt


2. Run the main script:

python main.py


This will execute the entire data processing pipeline and display the results of each step in the terminal.

Note: Make sure you have the necessary Spark configurations set up, and the Mackaroo API key is correctly provided in the `main.py` file before running the script.
