#!/usr/bin/env python3
"""
ETL Monday.com → BigQuery - Fix Finale Date
Risolve problema formato date per BigQuery
"""

import requests
import json
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import logging

# Configurazione
API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjIzNDU4MjgxNiwiYWFpIjoxMSwidWlkIjozNzYxNDgwNSwiaWFkIjoiMjAyMy0wMi0wNFQxNjo0MTo0Ny4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTQ1Nzk3NjYsInJnbiI6InVzZTEifQ.Baci-5D9rHkOGh9LoFHpxCM5spTHm4TJ1PekdC8Uk9c"
PROJECT_ID = "monday-reporting-1750794511"
DATASET_ID = "monday_data"
BOARD_ID = "8113598675"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_datetime(date_str):
    """Converte date Monday.com in formato BigQuery"""
    if not date_str:
        return None
    try:
        # Monday.com usa ISO format
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except:
        return None

def monday_api_call(query):
    """Chiamata API Monday.com"""
    url = "https://api.monday.com/v2"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, headers=headers, json={"query": query})
        response.raise_for_status()
        data = response.json()
        
        if 'errors' in data:
            logger.error(f"Errori API: {data['errors']}")
            return None
            
        return data
    except Exception as e:
        logger.error(f"Errore API: {e}")
        return None

def extract_all_projects_and_subitems():
    """Estrazione completa con fix formato date"""
    
    logger.info("🚀 ESTRAZIONE COMPLETA CON FIX DATE")
    
    query = f"""
    query {{
        boards(ids: [{BOARD_ID}]) {{
            items_page(limit: 100) {{
                cursor
                items {{
                    id
                    name
                    created_at
                    updated_at
                    column_values {{
                        id
                        text
                        value
                    }}
                    subitems {{
                        id
                        name
                        created_at
                        updated_at
                        column_values {{
                            id
                            text
                            value
                            column {{
                                id
                                title
                                type
                            }}
                        }}
                    }}
                }}
            }}
        }}
    }}
    """
    
    data = monday_api_call(query)
    
    if not data:
        logger.error("❌ Errore estrazione")
        return False
    
    items = data['data']['boards'][0]['items_page']['items']
    logger.info(f"✅ Estratti {len(items)} progetti")
    
    # Processa i dati con fix date
    return process_and_load_data_fixed(items)

def process_and_load_data_fixed(items):
    """Processa e carica i dati con fix formato date"""
    
    logger.info("⚙️ ELABORAZIONE DATI CON FIX DATE...")
    
    extraction_date = datetime.now().date()
    extraction_timestamp = datetime.now()
    
    projects_data = []
    subitems_data = []
    
    for item in items:
        # Progetto - FIX DATE
        project_data = {
            'extraction_date': extraction_date,
            'extraction_timestamp': extraction_timestamp,
            'project_id': item['id'],
            'project_name': item['name'],
            'created_at': parse_datetime(item.get('created_at')),  # FIX
            'updated_at': parse_datetime(item.get('updated_at')),  # FIX
            'po': None,
            'data_avvio': None,
            'var_non_var': None,
            'circolo': None,
            'tipologia': None,
            'stato_pipeline': None,
            'aperto_chiuso': None
        }
        
        # Mappa campi progetti
        for col_value in item['column_values']:
            col_id = col_value['id']
            text_value = col_value.get('text', '')
            
            if col_id == 'person' and text_value:
                project_data['po'] = text_value
            elif col_id == 'date4' and text_value:
                try:
                    project_data['data_avvio'] = datetime.strptime(text_value, '%Y-%m-%d').date()
                except:
                    pass
            elif col_id == 'status__1' and text_value:
                project_data['var_non_var'] = text_value
            elif col_id == 'status_1' and text_value:
                project_data['circolo'] = text_value
            elif col_id == 'status0' and text_value:
                project_data['tipologia'] = text_value
            elif col_id == 'status1' and text_value:
                project_data['stato_pipeline'] = text_value
            elif col_id == 'status6' and text_value:
                project_data['aperto_chiuso'] = text_value
        
        projects_data.append(project_data)
        
        # Subitem - FIX DATE
        if 'subitems' in item and item['subitems']:
            for subitem in item['subitems']:
                subitem_data = {
                    'extraction_date': extraction_date,
                    'extraction_timestamp': extraction_timestamp,
                    'subitem_id': subitem['id'],
                    'project_id': item['id'],
                    'subitem_name': subitem['name'],
                    'created_at': parse_datetime(subitem.get('created_at')),  # FIX
                    'updated_at': parse_datetime(subitem.get('updated_at')),  # FIX
                    'po': None,
                    'timeline_start': None,
                    'timeline_end': None,
                    'revenue_amount': 0.0,  # Assicura float
                    'status': None,
                    'tipologia': None
                }
                
                # Mappa campi subitem
                for col_value in subitem['column_values']:
                    col_type = col_value['column']['type']
                    text_value = col_value.get('text', '')
                    
                    if col_type == 'numbers' and text_value:
                        try:
                            subitem_data['revenue_amount'] = float(text_value)
                        except:
                            pass
                    elif col_type == 'person' and text_value:
                        subitem_data['po'] = text_value
                    elif col_type == 'timeline' and text_value:
                        try:
                            dates = text_value.split(' - ')
                            if len(dates) == 2:
                                subitem_data['timeline_start'] = datetime.strptime(dates[0], '%Y-%m-%d').date()
                                subitem_data['timeline_end'] = datetime.strptime(dates[1], '%Y-%m-%d').date()
                        except:
                            pass
                    elif col_type == 'status' and text_value:
                        if not subitem_data['status']:
                            subitem_data['status'] = text_value
                
                subitems_data.append(subitem_data)
    
    logger.info(f"📊 ELABORATI:")
    logger.info(f"   Progetti: {len(projects_data)}")
    logger.info(f"   Subitem: {len(subitems_data)}")
    
    revenue_subitems = len([s for s in subitems_data if s['revenue_amount'] > 0])
    total_revenue = sum(s['revenue_amount'] for s in subitems_data)
    
    logger.info(f"   Subitem con ricavi: {revenue_subitems}")
    logger.info(f"   Ricavi totali: €{total_revenue:,.2f}")
    
    # Carica in BigQuery con schema esplicito
    return load_to_bigquery_with_schema(projects_data, subitems_data)

def load_to_bigquery_with_schema(projects_data, subitems_data):
    """Carica dati in BigQuery con schema esplicito"""
    
    logger.info("📤 CARICAMENTO IN BIGQUERY CON SCHEMA...")
    
    client = bigquery.Client(project=PROJECT_ID)
    
    try:
        # Schema progetti
        projects_schema = [
            bigquery.SchemaField("extraction_date", "DATE"),
            bigquery.SchemaField("extraction_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("project_id", "STRING"),
            bigquery.SchemaField("project_name", "STRING"),
            bigquery.SchemaField("po", "STRING"),
            bigquery.SchemaField("data_avvio", "DATE"),
            bigquery.SchemaField("var_non_var", "STRING"),
            bigquery.SchemaField("circolo", "STRING"),
            bigquery.SchemaField("tipologia", "STRING"),
            bigquery.SchemaField("stato_pipeline", "STRING"),
            bigquery.SchemaField("aperto_chiuso", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP")
        ]
        
        # Schema subitem
        subitems_schema = [
            bigquery.SchemaField("extraction_date", "DATE"),
            bigquery.SchemaField("extraction_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("subitem_id", "STRING"),
            bigquery.SchemaField("project_id", "STRING"),
            bigquery.SchemaField("subitem_name", "STRING"),
            bigquery.SchemaField("po", "STRING"),
            bigquery.SchemaField("timeline_start", "DATE"),
            bigquery.SchemaField("timeline_end", "DATE"),
            bigquery.SchemaField("revenue_amount", "FLOAT64"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("tipologia", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP")
        ]
        
        # Progetti
        if projects_data:
            df_projects = pd.DataFrame(projects_data)
            
            # Tabella corrente
            projects_table = f"{PROJECT_ID}.{DATASET_ID}.projects"
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                schema=projects_schema
            )
            job = client.load_table_from_dataframe(df_projects, projects_table, job_config=job_config)
            job.result()
            
            # Tabella storica
            projects_historical_table = f"{PROJECT_ID}.{DATASET_ID}.projects_historical"
            job_config_hist = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=projects_schema
            )
            job_hist = client.load_table_from_dataframe(df_projects, projects_historical_table, job_config_hist)
            job_hist.result()
            
            logger.info(f"✅ Progetti caricati: {len(projects_data)}")
        
        # Subitem
        if subitems_data:
            df_subitems = pd.DataFrame(subitems_data)
            
            # Tabella corrente
            subitems_table = f"{PROJECT_ID}.{DATASET_ID}.project_subitems"
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                schema=subitems_schema
            )
            job = client.load_table_from_dataframe(df_subitems, subitems_table, job_config=job_config)
            job.result()
            
            # Tabella storica
            subitems_historical_table = f"{PROJECT_ID}.{DATASET_ID}.subitems_historical"
            job_config_hist = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=subitems_schema
            )
            job_hist = client.load_table_from_dataframe(df_subitems, subitems_historical_table, job_config_hist)
            job_hist.result()
            
            logger.info(f"✅ Subitem caricati: {len(subitems_data)}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Errore caricamento BigQuery: {e}")
        return False

def main():
    """Main execution"""
    logger.info("🚀 ETL MONDAY.COM → BIGQUERY - FIX FINALE")
    logger.info("=" * 60)
    
    if extract_all_projects_and_subitems():
        logger.info("🎉 ETL COMPLETATO CON SUCCESSO!")
        
        # Verifica finale
        logger.info("🔍 Verifica finale...")
        try:
            client = bigquery.Client(project=PROJECT_ID)
            
            # Verifica progetti
            projects_query = f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{DATASET_ID}.projects`"
            projects_count = client.query(projects_query).to_dataframe().iloc[0]['count']
            
            # Verifica subitem
            subitems_query = f"""
            SELECT 
                COUNT(*) as total_subitems,
                COUNT(CASE WHEN revenue_amount > 0 THEN 1 END) as revenue_subitems,
                ROUND(SUM(revenue_amount), 2) as total_revenue
            FROM `{PROJECT_ID}.{DATASET_ID}.project_subitems`
            """
            subitems_result = client.query(subitems_query).to_dataframe().iloc[0]
            
            # Verifica storiche
            hist_query = f"""
            SELECT 
                'projects_historical' as table_name, COUNT(*) as count 
            FROM `{PROJECT_ID}.{DATASET_ID}.projects_historical`
            UNION ALL
            SELECT 'subitems_historical', COUNT(*) 
            FROM `{PROJECT_ID}.{DATASET_ID}.subitems_historical`
            """
            hist_result = client.query(hist_query).to_dataframe()
            
            logger.info(f"🎉 RISULTATI FINALI:")
            logger.info(f"   📊 Progetti: {projects_count}")
            logger.info(f"   📊 Subitem totali: {subitems_result['total_subitems']}")
            logger.info(f"   💰 Subitem con ricavi: {subitems_result['revenue_subitems']}")
            logger.info(f"   💰 Ricavi totali: €{subitems_result['total_revenue']:,.2f}")
            logger.info(f"   📚 Tabelle storiche:")
            for _, row in hist_result.iterrows():
                logger.info(f"      {row['table_name']}: {row['count']} record")
            
        except Exception as e:
            logger.warning(f"Errore verifica: {e}")
        
    else:
        logger.error("❌ ETL FALLITO")

if __name__ == "__main__":
    main()
