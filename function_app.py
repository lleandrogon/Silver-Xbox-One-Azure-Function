import pandas as pd
import numpy as np
import duckdb
import re
import os
import logging
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="silver")
def silver(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🚀 Iniciando pipeline ETL Bronze -> Silver...")

    KEY_VAULT_URL = os.environ.get("KEY_VAULT_URL", "https://kvxboxone.vault.azure.net/") 

    credential = DefaultAzureCredential()
    vault_client = SecretClient(vault_url = KEY_VAULT_URL, credential = credential)

    account_name = vault_client.get_secret("storage-account-name").value
    account_key = vault_client.get_secret("storage-account-key").value

    storage_options = {
        "account_name": account_name,
        "account_key": account_key
    }

    try:
        df = pd.read_csv(
            "abfs://bronze@lakexboxone.blob.core.windows.net/xbox_one_games.csv",
            storage_options = storage_options
        )

        logging.info("✅ Arquivo do data lake lido com sucesso!")
    except:
        logging.error("❌ Leitura do arquivo no data lake falhou!")

        return func.HttpResponse(
            "❌ Erro no processamento!",
            status_code = 500
        )

    df = duckdb.sql("""--sql
        SELECT
            gameid AS id,
            name,
            web AS web_link,
            publisher,
            developer,
            release AS release_date,
            platform,
            genre,
            hardware,
            notes,
            medium,
            size,
            "completion est" AS completion_estimated,
            links AS link_type,
            features
        FROM df
    """).df()

    data_map = {
        "yesterday": pd.Timestamp.now().floor("d") - pd.Timedelta(days = 1),
        "today": pd.Timestamp.now().floor("d")
    }

    release_series = df["release_date"].astype(str).str.strip().str.lower()
    df["release_date"] = release_series.map(data_map).fillna(
        pd.to_datetime(df["release_date"], format = "%d %B %Y", errors = "coerce")
    )

    df["release_date"] = df["release_date"].dt.date

    df["platform"] = (
        df["platform"] \
            .fillna("") \
            .str.replace(r"Windows \(Windows 10\+\)", "Windows", regex = True) \
            .str.replace(r"Windows \(Pending\)", "Windows", regex = True) \
            .str.replace(r"Nintendo Switch \(Windows 10\+\)", "Nintendo Switch", regex = True) \
    )

    platforms = (
        df["platform"]
        .str.get_dummies(sep = ", ") \
        .astype(bool)
    )

    df = pd.concat([df, platforms], axis = 1)

    df = df.drop(columns = ["platform"])

    hardware = (
        df["hardware"] \
            .fillna("") \
            .str.get_dummies(sep = ", ") \
            .astype(bool)
    )

    df = pd.concat([df, hardware], axis = 1)

    df = df.drop(columns = ["hardware"])

    (
        df["notes"] \
            .fillna("")
            .str.split(", ")
            .explode()
            .value_counts()
    )

    notes = (
        df["notes"] \
            .fillna("") \
            .str.get_dummies(sep = ", ") \
            .astype(bool) \
    )

    df = pd.concat([df, notes], axis = 1)

    df = df.drop(columns = ["notes"])

    def convert_size(size):
        if pd.isna(size):
            return np.nan

        if "GB" in size:
            return float(size.replace("GB", ""))

        if "MB" in size:
            return float(size.replace("MB", "")) / 1024

        return np.nan

    df["size_gb"] = df["size"].apply(convert_size)

    df = df.drop(columns = ["size"])

    df["completion_estimated_clean"] = (
        df["completion_estimated"] \
            .str.replace(r"\s*\(.*\)", "", regex = True)
    )

    df[["completion_min_hours", "completion_max_hours"]] = (
        df["completion_estimated_clean"] \
            .str.extract(r"(\d+)-(\d+)")
    )

    df["completion_min_hours"] = df["completion_min_hours"].astype(float)
    df["completion_max_hours"] = df["completion_max_hours"].astype(float)

    df = df.drop(columns = ["completion_estimated"])

    features = (
        df["features"] \
            .fillna("") \
            .str.get_dummies(sep = ", ") \
            .astype(bool) \
    )

    df = pd.concat([df, features], axis = 1)

    df = df.drop(columns = ["features"])

    df = df.loc[:, df.columns != ""]

    df.columns = (
        df.columns \
            .str.lower() \
            .str.strip() \
            .map(lambda x: re.sub(r"[^a-z0-9]+", "_", x)) \
            .str.strip("_")
    )

    logging.info("💾 Salvando Parquet na camada Silver...")

    df.to_parquet(
        "abfs://silver@lakexboxone.blob.core.windows.net/xbox_one_games.parquet",
        storage_options = storage_options
    )

    return func.HttpResponse(
            "✅ Dados da camada Bronze processados e gravados na camada Silver com sucesso!",
            status_code = 200
        )