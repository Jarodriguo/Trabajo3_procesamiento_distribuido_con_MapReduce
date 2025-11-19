from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
import csv
import os
from datetime import datetime


class WeatherMonthlyAgg(MRJob):

    # Protocolo para leer líneas crudas
    INPUT_PROTOCOL = RawValueProtocol

    # Protocolo para escribir solo valores limpios
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        # Ignorar las líneas de metadatos de Open-Meteo
        if line.startswith("latitude") or line.startswith("time,"):
            return

        # Detectar ciudad desde el nombre del archivo
        filename = os.environ.get("map_input_file", "")
        city = os.path.basename(filename).replace(".csv", "")

        # Parsear CSV
        try:
            row = next(csv.reader([line]))
        except Exception:
            return

        if len(row) < 4:
            return

        date_str, tmax, tmin, prec = row[:4]

        # Validar números
        try:
            tmax = float(tmax)
            tmin = float(tmin)
            prec = float(prec)
        except:
            return

        # Obtener YYYY-MM
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            year_month = date_obj.strftime("%Y-%m")
        except:
            return

        key = f"{city}|{year_month}"
        yield key, (tmax, tmin, prec, 1)

    def reducer(self, key, values):
        total_tmax = 0.0
        total_tmin = 0.0
        total_prec = 0.0
        count = 0

        for tmax, tmin, prec, c in values:
            total_tmax += tmax
            total_tmin += tmin
            total_prec += prec
            count += c

        avg_tmax = total_tmax / count if count else 0
        avg_tmin = total_tmin / count if count else 0

        city, year_month = key.split("|")

        # Resultado final SIN null y SIN comillas
        output = f"{city},{year_month},{avg_tmax:.2f},{avg_tmin:.2f},{total_prec:.2f}"
        yield None, output


if __name__ == "__main__":
    WeatherMonthlyAgg.run()