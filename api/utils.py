import csv
import os

RESULT_FILE = os.path.join(os.path.dirname(__file__), "data", "resultado.csv")


def load_data():
    """Cargar todos los datos del CSV resultado en una lista de dicts."""
    print(">>> Debug: Buscando archivo en:", RESULT_FILE)

    data = []
    if not os.path.exists(RESULT_FILE):
        return data

    with open(RESULT_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
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


def get_cities():
    """Obtener lista única de ciudades."""
    data = load_data()
    return sorted(list({d["city"] for d in data}))