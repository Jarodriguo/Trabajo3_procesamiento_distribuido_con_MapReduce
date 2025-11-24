# Trabajo3_procesamiento_distribuido_con_MapReduce
Trabajo 3 de la materia de tópicos especiales en telemática

Este es el proyecto final para la clase Tópicos Especiales en Telemática, enfocado en la arquitectura Batch.

La idea era montar un flujo de procesamiento distribuido completo, usando únicamente Hadoop MapReduce. Tuvimos que simular todas las etapas: desde cargar los datos, procesarlos en paralelo, y finalmente, servirlos para que alguien los pueda consumir.

## Datos

Archivos Procesados: Datos de [Clima/Temperaturas/Ventas, etc.] para cinco ciudades (Bogotá, Medellín, Cali, Buenos Aires y Madrid).

**Estos fueron procesados en formato CSV.**

## Tecnológías 

Para que todo esto funcionara, usamos varios servicios y herramientas clave:

- Cluster/Orquestación: AWS EMR (Elastic MapReduce). Lo usamos para crear un clúster de Hadoop listo para trabajar.
  
- Almacenamiento (Persistente): AWS S3. Aquí guardamos los datos originales (el raw data) y el resultado final del análisis.

- Almacenamiento (Distribuido): HDFS (Hadoop Distributed File System). Es el sistema de archivos interno de Hadoop, clave para que el MapReduce funcione rápido.

- Procesamiento: MapReduce con MRJob (Python). Implementamos la lógica de agregación (el weather_monthly_agg.py) en Python para correrlo sobre Hadoop.

- Entrega de Resultados: Archivo de CSV integrado y unificado luego de ejecutarse el proceso de Map Reduce.

### Estructura del Repositorio

├── api/                             # Código de la API (FastAPI)
│   ├── main.py                      # Aquí definimos los endpoints
│   ├── utils.py                     # Funciones de ayuda
│   └── __init__.py                  
├── data/                            # Datos de entrada originales
│   └── raw/
│       ├── bogota.csv
│       └── ...
├── MapReduce/                       # Scripts para el job MapReduce
│   └── weather_monthly_agg.py       # Nuestro algoritmo MapReduce (con MRJob)
├── .gitignore
├── README.md
└── requirements.txt                 # Dependencias (fastapi, uvicorn, mrjob, etc.)

## Los Comandos Clave

Estos son los comandos esenciales para levantar el ambiente y correr el procesamiento. Se asume que el clúster EMR está creado y los datos en S3.

### Conexión y Setup

Conexión SSH al Master: ssh -i emr-key.pem hadoop@ec2-44-220-70-92.compute-1.amazonaws.com

El DNS público puede variar si se necesita otro cluster debido al prematuro cierre de sesión de AWS.

### Copiar el código de la API

scp -i emr-key.pem -r api/ hadoop@ec2-44-203-220-6.compute-1.amazonaws.com:/home/hadoop/. 

### Flujo MapReduce (Dentro del EMR)

Primero, movemos los datos a HDFS, corremos el job, y luego consolidamos el resultado.

**Cargar CSV a HDFS:** 
hadoop fs -cp s3://scelisl-emr/data/*.csv /user/hadoop/data/

**Ejecutar MapReduce:** 
python3 weather_monthly_agg.py -r hadoop hdfs:///user/hadoop/data/*.csv --output-dir hdfs:///user/hadoop/weather_output

**Consolidar el resultado**
hdfs dfs -cat /user/hadoop/weather_output/part-* > weather_agg.csv.

Aquí lo que hacemos es unificar el proceso de Map Reduce a un solo archivo

**Levantar la API**

Con el resultado consolidado (weather_agg.csv) en el nodo maestro, la API puede leerlo y servirlo.Moverse a la carpeta api: cd api.

Iniciar el servidor Uvicorn: 
python -m uvicorn main:app --host 0.0.0.0 --port 8000.

### Acceso al Navegador

Una vez que Uvicorn esté running, se puede acceder a la API desde el navegador:

**Para ver los datos:** 
http://ec2-44-220-70-92.compute-1.amazonaws.com:8000/cities

**Para ver la documentación (Swagger):** 
http://ec2-44-220-70-92.compute-1.amazonaws.com:8000/docs 

### El Fix del Puerto 8000

Un punto clave en el deployment fue solucionar el error de Grupos de Seguridad al crear el clúster. AWS EMR no permite abrir puertos (diferentes a 22) al público. Para poder acceder a nuestra API en el puerto 8000, se tuvo que:

1. Crear una regla de entrada (Inbound Rule) TCP para el Puerto 8000.
2. Restringir el origen (Source) a mi IP pública específica (181.137.71.2/32), en lugar de dejarlo en 0.0.0.0/0 (público).
3. Esto garantizó el acceso a la API solo desde mi máquina sin que AWS nos bloqueara la creación del clúster.
