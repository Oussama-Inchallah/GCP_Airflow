from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from datetime import datetime
import requests
import pandas as pd

def  extract_and_load(**context):

    frames = []

    for country in ["MAR", "TUN", "DZA", "EGY", "FRA"]:
        for indicators in ["NY.GDP.MKTP.CD","FP.CPI.TOTL.ZG","SP.POP.TOTL","SL.UEM.TOTL.ZS","SP.DYN.LE00.IN"] : 
            url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicators}"

            response = requests.get(
                url,
                params={"format": "json", "per_page": 1000},
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            observations = data[1]
            df = pd.json_normalize(observations)

            if df.empty:
                raise ValueError("Aucune donnée récupérée depuis l'API World Bank")
            else:
                
                    # selection des colonnes souhaiter
                    df = df[[
                        "countryiso3code",
                        "country.value",
                        "indicator.id",
                        "indicator.value",
                        "date",
                        "value"
                    ]]

                    # renommage des colonnes a des noms des colonnes compatible 
                    # avec les colonnes du schema du tableau de bq
                    df.columns = [
                        "country_code",
                        "country_name",
                        "indicator_code",
                        "indicator_label",
                        "year",
                        "value",
                    ]
                    df["year"] = df["year"].astype("int64")
                    df["load_date"] = context["ds"]
                    frames.append(df)

    result = pd.concat(frames, ignore_index=True)

    local_path = "./data/indicators.csv"
    result.to_csv(local_path, index=False)

    return local_path

with DAG(
    "worldbank_gdp_ingestion", 
    schedule_interval="@daily", 
    start_date=datetime(2026, 11, 1)
    ) as dag:
    
    extract_and_upload = PythonOperator(
        task_id="extract_and_load_locally",
        python_callable=extract_and_load,
    )
    
    
    

    # Upload the CSV file to Google Cloud Storage
    # client = storage.Client()
    # bucket_name = "<project>-worldbank-raw"
    # bucket = client.bucket(bucket_name)
    
    # Assuming the extracted CSV file is named 'API_NY.GDP.MKTP.CD_DS2_en_csv_v2_535045.csv'
    # csv_file_path = "/tmp/API_NY.GDP.MKTP.CD_DS2_en_csv_v2_535045.csv"
    
    # Create a folder in GCS based on the execution date
    # execution_date = datetime.now().strftime("%Y-%m-%d")
    # blob = bucket.blob(f"worldbank/{execution_date}/indicators.csv")
    
    # blob.upload_from_filename(csv_file_path)


#     load_to_bigquery = GCSToBigQueryOperator(
#         task_id="load_to_bigquery",
#         bucket="<project>-worldbank-raw",
#         source_objects=["worldbank/{{ ds }}/indicators.csv"],
#         destination_project_dataset_table="raw_worldbank.indicators",
#         write_disposition="WRITE_TRUNCATE",
#         source_format="CSV",
#         skip_leading_rows=1,
#     )
    
#     extract_and_upload >> load_to_bigquery
