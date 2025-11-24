from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import os
from utils import (
    load_data_from_s3,
    load_data_from_hdfs,
    extract_cities
)

app = FastAPI(
    title="API Meteorológica – EMR MapReduce",
    version="1.0",
    description="Lectura de datos agregados desde S3 y HDFS"
)

S3_BUCKET = "scelisl-emr"
S3_KEY = "output/weather_agg.csv"

HDFS_PATH = "/user/hadoop/weather_output_combined/weather_agg.csv"


def load_from_s3():
    try:
        return load_data_from_s3(S3_BUCKET, S3_KEY)
    except Exception as e:
        raise HTTPException(500, f"Error leyendo S3: {e}")


def load_from_hdfs():
    try:
        return load_data_from_hdfs(HDFS_PATH)
    except Exception as e:
        raise HTTPException(500, f"Error leyendo HDFS: {e}")


@app.get("/")
def root():
    return {"status": "OK", "message": "API funcionando correctamente"}


@app.get("/cities")
def get_cities():
    data = load_from_hdfs()
    return {"cities": extract_cities(data)}


@app.get("/city/{city}")
def city_data(city: str):
    data = load_from_hdfs()
    filtered = [d for d in data if d["city"].lower() == city.lower()]

    if not filtered:
        raise HTTPException(404, "Ciudad no encontrada")

    return filtered


@app.get("/city/{city}/{year_month}")
def city_by_month(city: str, year_month: str):
    data = load_from_hdfs()

    for d in data:
        if d["city"].lower() == city.lower() and d["month"] == year_month:
            return d

    raise HTTPException(404, "Registro no encontrado")


@app.get("/source/s3")
def test_s3():
    """Verifica lectura desde S3."""
    return load_from_s3()[:5]


@app.get("/source/hdfs")
def test_hdfs():
    """Verifica lectura desde HDFS."""
    return load_from_hdfs()[:5]