import os
import csv
import boto3
import mysql.connector

# Configuración MySQL desde variables de entorno
db_host     = os.getenv('MYSQL_HOST')
db_port     = int(os.getenv('MYSQL_PORT', '3306'))
db_user     = os.getenv('MYSQL_USER')
db_password = os.getenv('MYSQL_PASSWORD')
db_name     = os.getenv('MYSQL_DATABASE')
table_name  = os.getenv('MYSQL_TABLE', 'your_table')

# Nombre del CSV de salida
fichero_upload = 'data_mysql.csv'
# Bucket S3 (por defecto o desde variable de entorno)
nombre_bucket  = os.getenv('S3_BUCKET', 'blc-aoutput-01')

# Función para extraer datos de MySQL
def fetch_data():
    conn = mysql.connector.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    cursor.close()
    conn.close()
    return columns, rows

# Guardar los datos en CSV
def save_to_csv(columns, rows, filename):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    return filename

# Subir el CSV a S3
def upload_to_s3(file_path):
    s3 = boto3.client('s3')
    s3.upload_file(file_path, nombre_bucket, file_path)
    print(f"Ingesta completada: {file_path} subido a s3://{nombre_bucket}/{file_path}")

if __name__ == '__main__':
    cols, data = fetch_data()
    csv_file   = save_to_csv(cols, data, fichero_upload)
    upload_to_s3(csv_file)