# weather_script.py

#Se importan las librerias necesarias

import requests
import pandas as pd
from tabulate import tabulate
import pytz
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging

# Función para chequeo de alertas

def check_temperature_alerts(dataframe, temp_limits):
    alerts = []
    for index, row in dataframe.iterrows():
        if row['temp2m'] > temp_limits['max_temp'] or row['temp2m'] < temp_limits['min_temp']:
            timestamp_str = str(row['timestamp'])
            alerts.append(f"Temperature alert! {row['city_name']} at {timestamp_str} has a temperature of {row['temp2m']}°C.")
    return alerts

# Función para enviar mails con alertas

def send_email_alert(alerts, email_settings):

    from_email = email_settings['from_email']
    from_password = email_settings['from_password']
    to_email = email_settings['to_email']

    subject = "Weather Alert"
    body = "The following cities have temperatures alerts:\n\n"
    
    for alert in alerts:
        body += f"{alert}\n"

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    
    msg.attach(MIMEText(body, 'plain'))
    
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(from_email, from_password)
        text = msg.as_string()
        server.sendmail(from_email, to_email, text)
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {str(e)}")

# Función para tomar data del clima

def get_weather_data(lon, lat, city_name):
    # Se contruye la URL en función de las coordenadas
    url = f"https://www.7timer.info/bin/astro.php?lon={lon}&lat={lat}&ac=0&unit=metric&output=json&tzshift=0"

    # Se obtiene el JSON desde la URL
    response = requests.get(url)
    data = response.json()

    # Se convierte el JSON en un DataFrame
    df = pd.DataFrame(data['dataseries'])

    # Se separa el diccionario dentro de la columna 'wind10m' en columnas separadas
    wind_data = pd.json_normalize(df['wind10m'])

    # Se renombra las columnas resultantes
    wind_data.columns = ['wind_direction', 'wind_speed']

    # Se elimina la columna original 'wind10m' del DataFrame
    df.drop(columns=['wind10m'], inplace=True)

    # Se concatena las nuevas columnas al DataFrame original
    df = pd.concat([df, wind_data], axis=1)

    # Se agrega columna de tiempo en formato de fecha y hora
    df['timestamp'] = pd.to_datetime(data['init'], format='%Y%m%d%H')

    # Se agrega columna con el nombre de la ciudad
    df['city_name'] = city_name

    # Se crea la columna 'unique_id' concatenando 'timestamp', 'timepoint' y 'city_name'
    df['unique_id'] = df['timestamp'].astype(str) + '_' + df['timepoint'].astype(str) + '_' + df['city_name']

    print('Data retrieved from API successfully!')

    return df

# Función para cargar datos en Redshift
def cargar_en_redshift(conn, tabla, dataframe):
    try:
        # Se obtiene los tipos de datos de las columnas del DataFrame
        dtypes = dataframe.dtypes
        cols = list(dtypes.index)
        tipos = list(map(str, dtypes.values))
        # Se Mapea los tipos de datos de Pandas a tipos de datos de Redshift
        type_map = {'int64': 'INT', 'float64': 'FLOAT', 'object': 'VARCHAR(50)', 'datetime64[ns]': 'VARCHAR(50)'}
        # Se obtiene los tipos de datos de Redshift correspondientes
        sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
        # Se define formato SQL para las columnas
        column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
        # Definición de la clave primaria
        primary_key = "unique_id"
        # Se combina las definiciones de columnas en la sentencia CREATE TABLE
        table_schema = f"""
            CREATE TABLE IF NOT EXISTS {tabla} (
                {', '.join(column_defs)},
                PRIMARY KEY ({primary_key})
            );
            """
        # Se crea la tabla en Redshift
        cur = conn.cursor()
        cur.execute(table_schema)
        conn.commit()
        # Se genera los valores a insertar
        values = [tuple(x) for x in dataframe.to_numpy()]
        # Se define la sentencia INSERT
        insert_sql = f"INSERT INTO {tabla} ({', '.join(cols)}) VALUES %s"
        # Se ejecuta la transacción para insertar/actualizar los datos
        cur.execute("BEGIN")
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT")
        print('Proceso de carga en Redshift terminado')
    except Exception as e:
        print(f"Error al cargar datos en Redshift: {str(e)}")

# Función para conectar a redshift

def conectar_redshift():
   # Obtiene la ruta completa al archivo secret.json
  secret_file_path = 'secret.json' 

  try:
      # Se obtiene el secreto
      with open(secret_file_path) as f:
            secreto = json.load(f)
      # Se conecta a Redshift utilizando las credenciales
      conn = psycopg2.connect(
          dbname=secreto['dbname'],
          user=secreto['user'],
          password=secreto['password'],
          host=secreto['host'],
          port=secreto['port']
      )
      print("Connected to Redshift successfully!")
      return conn

  except psycopg2.Error as e:
        print("Unable to connect to Redshift.")
        print(e)
        return None

  except FileNotFoundError as fnf_error:
        print("El archivo secret.json no se encontró en la ubicación especificada.")
        print(fnf_error)
        return None

# Función para comenzar el trabajo diario de tomar información del clima

def run_weather_etl():

    file_path = os.path.join(os.path.dirname(__file__),'config_alertas.json')

    # Cargar configuración de alertas desde archivo JSON
    with open(file_path) as config_file:
        config = json.load(config_file)

    # Se obtienen datos meteorológicos para diferentes ubicaciones (editable desde config_alertas.json)
    locations = config['cities']
    temp_limits = config['temperature_limits']

    # Lista para almacenar los DataFrames individuales de cada ubicación
    all_weather_data = []

    # Se obtiene los datos meteorológicos para cada ubicación y se agrega a un DataFrame general
    for location in locations:
        weather_df = get_weather_data(location['lon'], location['lat'], location['name'])
        all_weather_data.append(weather_df)

    # Se combina todos los DataFrames en uno solo
    combined_weather_df = pd.concat(all_weather_data)

    # Validación de nulos y valores atípicos
    combined_weather_df.dropna(inplace=True)

    # Se conecta y carga datos en Redshift
    conn = conectar_redshift()

    if conn:
        cargar_en_redshift(conn=conn, tabla='tabla_temperatura', dataframe=combined_weather_df)
        conn.close()

    # Se convierte Timestamp a string después de la carga en Redshift para utilizar en xCom
    combined_weather_df['timestamp'] = combined_weather_df['timestamp'].astype(str)

    # Se convierte el dataframe en diccionario y con solo 3 columnas a analizar en las siguientes tareas
    combined_weather_df=combined_weather_df[['city_name', 'timestamp', 'temp2m']].to_dict(orient='records')

    return combined_weather_df

if __name__ == "__main__":
    run_weather_etl()
