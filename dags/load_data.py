from datetime import datetime
import os
import requests
import pandas as pd
from dotenv import load_dotenv
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

logging.basicConfig(level=logging.INFO)
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
if not PROJECT_ID:
    raise ValueError("PROJECT_ID doit être défini dans l'environnement Airflow")

BUCKET_NAME = os.getenv("GCS_BUCKET")
if not BUCKET_NAME:
    raise ValueError("GCS_BUCKET doit être défini dans l'environnement Airflow")

GCS_OBJECT = "worldbank/{{ ds }}/indicators.csv"

def extract_and_upload(**context):
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
                    df["load_date"] = context["ds"]  # temps d'éxecution du DAG
                    frames.append(df)

    result = pd.concat(frames, ignore_index=True)
    logging.info(f"Nombre de lignes extraites : {len(result)}")

    local_path = "./data/indicators.csv"
    result.to_csv(local_path, index=False)
    logging.info(f"Fichier CSV sauvegardé localement à : {local_path}")

    gcs_object = f"worldbank/{context['ds']}/indicators.csv"
    
    GCSHook(gcp_conn_id="google_cloud_default").upload(
        bucket_name=BUCKET_NAME,
        object_name=gcs_object,
        filename=local_path)
    logging.info(f"Fichier CSV téléchargé sur GCS : {gcs_object}")
        
with DAG(
    "worldbank_gdp_ingestion",
    schedule="@daily",
    start_date=datetime(2026, 11, 2),
    catchup=False,
) as dag:

    extract_and_upload = PythonOperator(
        task_id="extract_and_upload_gcs",
        python_callable=extract_and_upload,
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=[GCS_OBJECT],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.raw_worldbank.indicators"
        ),
        schema_fields=[
            {"name": "country_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "indicator_code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "indicator_label", "type": "STRING", "mode": "NULLABLE"},
            {"name": "year", "type": "INT64", "mode": "NULLABLE"},
            {"name": "value", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "load_date", "type": "DATE", "mode": "NULLABLE"},
        ],
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        time_partitioning={"type": "DAY", "field": "load_date"},
        cluster_fields=["country_code"],
    )

    extract_and_upload >> load_to_bigquery
