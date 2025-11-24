import boto3
import csv
import subprocess

def load_data_from_s3(bucket: str, key: str):
    """
    Descarga el archivo CSV desde S3 y lo convierte a estructura Python.
    """
    s3 = boto3.client("s3")

    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8").splitlines()

    return parse_csv_content(content)


def load_data_from_hdfs(hdfs_path: str):
    """
    Utiliza 'hdfs dfs -cat' para leer archivos desde HDFS (solo funciona dentro del nodo master).
    """

    result = subprocess.run(
        ["hdfs", "dfs", "-cat", hdfs_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if result.returncode != 0:
        raise Exception(f"Error leyendo HDFS: {result.stderr}")

    content = result.stdout.splitlines()
    return parse_csv_content(content)


def parse_csv_content(lines):
    """
    Convierte el contenido del CSV a lista de diccionarios.
    Asume formato: city,year_month,tmax,tmin,precip
    """

    data = []
    reader = csv.reader(lines)

    for row in reader:
        if len(row) < 5:
            continue

        city, year_month, tmax, tmin, prec = row

        try:
            data.append({
                "city": city,
                "month": year_month,
                "tmax": float(tmax),
                "tmin": float(tmin),
                "precip": float(prec)
            })
        except:
            continue

    return data

def extract_cities(data):
    return sorted(list({d["city"] for d in data}))