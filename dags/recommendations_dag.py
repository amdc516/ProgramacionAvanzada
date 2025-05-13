from airflow.decorators import dag, task
from airflow.configuration import conf
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, date
import psycopg2
import logging
import pandas as pd
from pathlib import Path
from google.cloud import storage
import io

logger = logging.getLogger(__name__)

# Rutas a archivos
key_path = next(Path(".").rglob("triple-circle-457622-u6-e72c84a8574f.json"))


bucket_name = "tp-final"
file_names = [
    "raw_data/ads_views",
    "raw_data/advertiser_ids",
    "raw_data/product_views"
]


@dag(
    dag_id="process_csv_files",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["csv", "processing"]
)
def csv_processing_pipeline():

    @task()
    def leer_csv_desde_gcs(file_name: str):
        storage_client = storage.Client.from_service_account_json(key_path)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_text()
        df = pd.read_csv(io.StringIO(content))
        return df.to_dict()


    @task()
    def FiltrarDatos(info, data):
        # Obtener la fecha de ayer
        yesterday = (datetime.now() - timedelta(days=1)).date()

        # Leer los datos
        #data = pd.read_csv(path_csv)
        data = pd.DataFrame(data)
        advertisers_activos = pd.DataFrame(info)
        logger.info(f'this is the info: {advertisers_activos}')
        #advertisers_activos = pd.read_csv(advertiser_ids_path)
        advertisers_activos_list = advertisers_activos['advertiser_id'].tolist()

        logger.info(f'These are the active advertisers of the day: {advertisers_activos_list}')

        # Asegurarse de que la columna 'date' esté en formato datetime.date
        data['date'] = pd.to_datetime(data['date']).dt.date

        # Filtrar filas con advertisers activos y con fecha de ayer
        filtered = data[
            (data['advertiser_id'].isin(advertisers_activos_list)) &
            (data['date'] == yesterday)
        ]

        logger.info(f'Estos son los datos filtrados (advertisers activos y fecha de ayer):\n{filtered}')
        #cada diccionario del return es una fila
        return {
            "filtered_today": filtered.to_dict(orient="records"),  # Lista de dicts por fila
            "advertisers_list": advertisers_activos_list
        }


    @task()
    def TopCTR(data_dict: dict):
        df = pd.DataFrame(data_dict["filtered_today"])

        # Filtrar solo clicks e impresiones
        df = df[df['type'].isin(['click', 'impression'])].copy()

        if df.empty:
            return {}

        # Contar clicks e impresiones por advertiser y producto
        grouped = (
            df.groupby(['advertiser_id', 'product_id', 'type'])
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )

        # Asegurar columnas
        grouped['click'] = grouped.get('click', 0)
        grouped['impression'] = grouped.get('impression', 0)

        # Filtrar solo con impresiones > 0 y calcular CTR
        grouped = grouped[grouped['impression'] > 0].copy()
        grouped['ctr'] = (grouped['click'] / grouped['impression']) * 100

        # Obtener el top 20 por advertiser
        top_ctr_per_advertiser = (
            grouped.sort_values(['advertiser_id', 'ctr'], ascending=[True, False])
            .groupby('advertiser_id')
            .head(20)
            .reset_index(drop=True)
        )

        # Devolver como dict por advertiser_id
        result = {
            str(advertiser): group.drop(columns='advertiser_id').to_dict(orient='records')
            for advertiser, group in top_ctr_per_advertiser.groupby('advertiser_id')
        }

        return result


    @task()
    def TopProduct(data_dict: dict, top_n: int = 20):       

        df = pd.DataFrame(data_dict["filtered_today"])

        # Contar vistas por producto y advertiser
        product_counts = (
            df.groupby(['advertiser_id', 'product_id'])
            .size()
            .reset_index(name='views')
        )

        logger.info(f'These are the product counts:\n{product_counts}')

        # Top N productos por advertiser
        top_products = (
            product_counts
            .sort_values(['advertiser_id', 'views'], ascending=[True, False])
            .groupby('advertiser_id')
            .head(top_n)
            .reset_index(drop=True)
        )

        # Estructura dict por advertiser_id
        result = {
            str(advertiser): group.drop(columns='advertiser_id').to_dict(orient='records')
            for advertiser, group in top_products.groupby('advertiser_id')
        }

        return result

     
    @task()
    def DBWriting(top_ctr: dict, top_product: dict):
        try:
            hook = PostgresHook(postgres_conn_id='postgres')
            conn = hook.get_conn()
            cursor = conn.cursor()

            today = date.today()

            # Crear tabla si no existe: top_ctr
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS top_ctr (
                advertiser_id VARCHAR(50),
                product_id VARCHAR(50),
                click INTEGER,
                impression INTEGER,
                ctr FLOAT,
                insert_date DATE
            )
            """)

            # Insertar datos en top_ctr
            for advertiser_id, products in top_ctr.items():
                for item in products:
                    cursor.execute("""
                        INSERT INTO top_ctr (advertiser_id, product_id, click, impression, ctr, insert_date)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        advertiser_id,
                        item['product_id'],
                        item['click'],
                        item['impression'],
                        item['ctr'],
                        today
                    ))

            # Crear tabla si no existe: top_products
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS top_products (
                advertiser_id VARCHAR(50),
                product_id VARCHAR(50),
                views INTEGER,
                insert_date DATE
            )
            """)

            # Insertar datos en top_products
            for advertiser_id, products in top_product.items():
                for item in products:
                    cursor.execute("""
                        INSERT INTO top_products (advertiser_id, product_id, views, insert_date)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        advertiser_id,
                        item['product_id'],
                        item['views'],
                        today
                    ))

            conn.commit()
            logger.info("Datos escritos correctamente en la base de datos con fecha.")

        except Exception as e:
            logger.error(f"Error al escribir en la base de datos: {e}")
            conn.rollback()

        finally:
            cursor.close()
            conn.close()

    # Pipeline
    advertisers_ids = leer_csv_desde_gcs.override(task_id="leer_advertiser_ids")(file_names[1]) # devuelve dict key index value adv id
    ads_views = leer_csv_desde_gcs.override(task_id="leer_ads_views")(file_names[0])
    product_views = leer_csv_desde_gcs.override(task_id="leer_product_views")(file_names[2])
    filtered_ads_views_data = FiltrarDatos.override(task_id="filtrar_ads_views")(advertisers_ids, ads_views)
    filtered_product_views_data = FiltrarDatos.override(task_id="filtrar_product_views")(advertisers_ids, product_views)

    top_20_ctr = TopCTR(filtered_ads_views_data)
    top_20_av = TopProduct(filtered_product_views_data)
    DBWriting(top_20_ctr, top_20_av)   


csv_processing_pipeline()
