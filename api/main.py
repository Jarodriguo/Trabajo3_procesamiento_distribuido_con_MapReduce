from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from api.utils import load_data, get_cities
import os

app = FastAPI(
    title="API de Datos Meteorológicos (MapReduce)",
    description="Exposición del resultado procesado con Hadoop MapReduce",
    version="1.0"
)

RESULT_FILE = os.path.join(os.path.dirname(__file__), "data", "resultado.csv")


@app.get("/")
def root():
    return {"status": "OK", "message": "API funcionando correctamente"}


@app.get("/cities")
def list_cities():
    cities = get_cities()
    return {"cities": cities}


@app.get("/city/{city}")
def get_city(city: str):
    data = load_data()
    filtered = [d for d in data if d["city"].lower() == city.lower()]
    if not filtered:
        raise HTTPException(status_code=404, detail="Ciudad no encontrada")
    return filtered


@app.get("/city/{city}/{year_month}")
def get_city_by_month(city: str, year_month: str):
    data = load_data()
    for d in data:
        if d["city"].lower() == city.lower() and d["month"] == year_month:
            return d

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

@app.get("/city/{city}/year/{year}")
def get_city_by_year(city: str, year: str):
    data = load_data()

    # Filtrar registros donde el mes empieza por el año
    filtered = [
        d for d in data
        if d["city"].lower() == city.lower() and d["month"].startswith(year)
    ]

    if not filtered:
        raise HTTPException(status_code=404, detail="Registro no encontrado")

    return filtered