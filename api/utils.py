import csv
import boto3
from hdfs import InsecureClient
from io import StringIO
import os

# Ruta HDFS (modificar si tu carpeta cambia)
HDFS_URI = "http://localhost:9870"
HDFS_PATH = "/user/hadoop/weather_output_combined/weather_agg.csv"

# Ruta S3 (modificar con tu bucket)
S3_BUCKET = "scelisl-emr"
S3_KEY = "output/weather_agg.csv"

# Archivo local opcional
LOCAL_PATH = os.path.join(os.path.dirname(__file__), "data", "resultado.csv")


def parse_csv_text(text):
    data = []
    reader = csv.reader(text.splitlines())

    for row in reader:
        if len(row) != 5:
            continue

        city, year_month, tmax, tmin, prec = row

        data.append({
            "city": city,
            "month": year_month,
            "avg_temp_max": float(tmax),
            "avg_temp_min": float(tmin),
            "precipitation_total": float(prec)
        })

    return data


def load_from_hdfs():
    try:
        client = InsecureClient(HDFS_URI, user="hadoop")
        with client.read(HDFS_PATH, encoding="utf-8") as f:
            text = f.read()
        return parse_csv_text(text)
    except Exception as e:
        print("ERROR leyendo desde HDFS:", e)
        return []


def load_from_s3():
    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        text = obj["Body"].read().decode("utf-8")
        return parse_csv_text(text)
    except Exception as e:
        print("ERROR leyendo desde S3:", e)
        return []


def load_from_local():
    if not os.path.exists(LOCAL_PATH):
        return []

    with open(LOCAL_PATH, "r", encoding="utf-8") as f:
        return parse_csv_text(f.read())


def load_data(source="hdfs"):
    source = source.lower()

    if source == "hdfs":
        return load_from_hdfs()

    if source == "s3":
        return load_from_s3()

    if source == "local":
        return load_from_local()

    raise ValueError("Fuente inválida. Usa: hdfs, s3 o local.")


def get_cities(source="hdfs"):
    data = load_data(source)
    return sorted({d["city"] for d in data})