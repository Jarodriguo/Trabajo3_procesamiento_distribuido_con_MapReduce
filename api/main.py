from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from utils import load_data, get_cities
import os

app = FastAPI(
    title="API de Datos Meteorológicos (MapReduce + HDFS + S3)",
    description="Expone los resultados agregados generados por MapReduce desde HDFS o S3",
    version="2.0"
)

# Archivo local opcional
RESULT_FILE = os.path.join(os.path.dirname(__file__), "data", "resultado.csv")


@app.get("/")
def root():
    return {
        "status": "OK",
        "message": "API funcionando correctamente",
        "endpoints": [
            "/cities",
            "/city/{city}",
            "/city/{city}/{year_month}",
            "/download/csv"
        ]
    }


@app.get("/cities")
def list_cities(source: str = Query("hdfs", enum=["hdfs", "s3", "local"])):
    cities = get_cities(source)
    return {"source": source, "cities": cities}


@app.get("/city/{city}")
def get_city(city: str, source: str = Query("hdfs", enum=["hdfs", "s3", "local"])):
    data = load_data(source)

    filtered = [d for d in data if d["city"].lower() == city.lower()]

    if not filtered:
        raise HTTPException(status_code=404, detail="Ciudad no encontrada")

    return {"source": source, "records": filtered}


@app.get("/city/{city}/{year_month}")
def get_city_by_month(city: str, year_month: str,
                      source: str = Query("hdfs", enum=["hdfs", "s3", "local"])):
    data = load_data(source)

    for d in data:
        if d["city"].lower() == city.lower() and d["month"] == year_month:
            return {"source": source, "data": d}

    raise HTTPException(404, "Registro no encontrado")


@app.get("/download/csv")
def download_csv():
    if not os.path.exists(RESULT_FILE):
        raise HTTPException(status_code=404, detail="Archivo no encontrado")
    return FileResponse(
        RESULT_FILE,
        media_type="text/csv",
        filename="resultado.csv"
    )