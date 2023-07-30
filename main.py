from app import read_data, df_imputer, df_scaler, df_encoder, cluester_generator

def main():
    # Show the first few rows to check the data
    PATH = "dataset/eda.csv"
    # Initialize SparkSession
    df = read_data(PATH)
    df.show()
    print("######################################################################")
    print("1. Step is Done")
    print("######################################################################")

    # Columns with possible missing values in the DataFrame
    numeric_columns = ['age', 'ads_click_count', 'ads_price']
    categorical_columns = ['gender','country']

    df_imputed = df_imputer(df=df,numeric_columns=numeric_columns,categorical_columns=categorical_columns)
    print(df_imputed.show(5))

    print("######################################################################")
    print("2. Step is Done")
    print("######################################################################")

    # Columns to be scaled
    feature_cols = ['age_imputed', 'ads_click_count_imputed', 'ads_price_imputed']
    df_scaled = df_scaler(df_imputed,feature_cols)
    print(df_scaled.show(5))

    print("######################################################################")
    print("3. Step is Done")
    print("######################################################################")

    # Columns to be encoded
    categorical_columns = ['country', 'gender']
    df_encoded = df_encoder(df_scaled,categorical_columns)
    df_encoded.printSchema()
    df_encoded.show(5)

    print("columns of df_encoded: ",df_encoded.columns)
    print("######################################################################")
    print("4. Step is Done")
    print("######################################################################")

    # Select only the specified columns
    selected_columns_for_clustering = ['customer_id', 'age_imputed', 'ads_click_count_imputed', 'ads_price_imputed', 'scaled_features', 'country_encoded', 'gender_encoded']
    clustered_data = cluester_generator(df_encoded,selected_columns_for_clustering)
    # Show the clustering results
    clustered_data.select('customer_id', 'gender_encoded', 'country_encoded', 'scaled_features', 'prediction').show(20,truncate=False)

if __name__=='__main__':
    main()