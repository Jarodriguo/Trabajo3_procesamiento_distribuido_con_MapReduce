from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
import csv
import os
from datetime import datetime


class WeatherMonthlyAgg(MRJob):

    # Leer líneas tal como vienen (raw), sin intentar parseo automático
    INPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):

        # Omitir encabezados de metadatos y encabezado de datos
        if line.startswith("latitude") or line.startswith("time,"):
            return

        # Obtener el nombre del archivo procesado → permite múltiples ciudades
        filename = os.environ.get("map_input_file", "")
        city = os.path.basename(filename).replace(".csv", "")

        # Parseo manual del CSV
        try:
            row = next(csv.reader([line]))
        except Exception:
            return

        if len(row) < 4:
            return

        date_str, tmax, tmin, prec = row[:4]

        # Convertir strings a números
        try:
            tmax = float(tmax)
            tmin = float(tmin)
            prec = float(prec)
        except:
            return

        # Extraer año-mes
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            year_month = date_obj.strftime("%Y-%m")
        except:
            return

        key = f"{city}|{year_month}"

        # Emitimos datos crudos para agregación
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

        # CSV limpio final
        result = f"{city},{year_month},{avg_tmax:.2f},{avg_tmin:.2f},{total_prec:.2f}"

        yield None, result


if __name__ == "__main__":
    WeatherMonthlyAgg.run()