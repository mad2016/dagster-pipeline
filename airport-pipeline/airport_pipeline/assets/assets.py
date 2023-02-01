import pandas as pd
import requests
import io
from dagster import MetadataValue, Output, asset, OpExecutionContext
import os
import csv
import base64


@asset
def airline_json():
    """
    Get airline data from the  endpoint.
    """
    airline_json_data = requests.get(
        "https://think.cs.vt.edu/corgis/datasets/json/airlines/airlines.json"
    ).json()
    return airline_json_data


@asset(required_resource_keys={"s3"})
def airline_csv_data(context: OpExecutionContext) -> None:
    airline_json_data = airline_json()
    df = pd.json_normalize(airline_json_data)
    df.columns = df.columns.str.replace("[#]", "numb")
    df.columns = df.columns.str.replace("[ ]", "_")
    df.columns = df.columns.str.replace("[.]", "_")
    df["Airport_Name"] = df["Airport_Name"].str.replace(",", ";")
    df["Statistics_Carriers_Names"] = df[
        "Statistics_Carriers_Names"
    ].str.replace(",", ";")

    # return df
    bucket_name = os.environ.get("S3_BUCKET")

    for (Time_Year), group in df.groupby(["Time_Year"]):
        print(Time_Year)
        file_name = f"{Time_Year}-airline.csv"
        buffer = group.to_csv(index=False).encode()
        context.resources.s3.upload_fileobj(
            io.BytesIO(buffer), bucket_name, file_name
        )
