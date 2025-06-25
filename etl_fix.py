#!/usr/bin/env python3
"""
ETL Monday.com → BigQuery - Query Corrette
Fix per problemi GraphQL API
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

# Board IDs
BOARDS = {
    "progetti": "8113598675",
    "personnel": "8192003697", 
    "travel": "8119051122",
    "suppliers": "8133760076"
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
            logger.error(f"Errori API Monday.com: {data['errors']}")
            return None
            
        return data
    except Exception as e:
        logger.error(f"Errore chiamata API: {e}")
        return None

def test_correct_board_query():
    """Test con sintassi GraphQL corretta"""
    
    # Proviamo diverse sintassi possibili
    queries_to_test = [
        # Sintassi 1: Senza limite
        f"""
        query {{
            boards(ids: [{BOARDS['progetti']}]) {{
                id
                name
                items_page {{
                    items {{
                        id
                        name
                    }}
                }}
            }}
        }}
        """,
        
        # Sintassi 2: Con cursore
        f"""
        query {{
            boards(ids: [{BOARDS['progetti']}]) {{
                id
                name
                items_page(limit: 5) {{
                    cursor
                    items {{
                        id
                        name
                    }}
                }}
            }}
        }}
        """,
        
        # Sintassi 3: Query V1 (deprecata ma potrebbe funzionare)
        f"""
        {{
            boards(ids: [{BOARDS['progetti']}]) {{
                id
                name
                items {{
                    id
                    name
                }}
            }}
        }}
        """,
        
        # Sintassi 4: Con complexity control
        f"""
        query GetBoard {{
            complexity {{
                before
                after
                query
            }}
            boards(ids: [{BOARDS['progetti']}]) {{
                id
                name
                items_page(limit: 3) {{
                    items {{
                        id
                        name
                    }}
                }}
            }}
        }}
        """
    ]
    
    for i, query in enumerate(queries_to_test, 1):
        logger.info(f"🧪 Test sintassi {i}...")
        
        data = monday_api_call(query)
        
        if data and 'data' in data and data['data']['boards']:
            logger.info(f"✅ Sintassi {i} funziona!")
            
            board = data['data']['boards'][0]
            logger.info(f"Board: {board['name']} (ID: {board['id']})")
            
            # Controlla la struttura items
            if 'items_page' in board:
                items = board['items_page']['items']
                logger.info(f"Trovati {len(items)} progetti con items_page")
            elif 'items' in board:
                items = board['items']
                logger.info(f"Trovati {len(items)} progetti con items")
            else:
                logger.info("Nessun campo items trovato")
                continue
            
            # Se abbiamo items, proviamo ad estrarre subitem
            if items:
                return test_subitem_extraction_with_syntax(i)
        else:
            logger.warning(f"❌ Sintassi {i} non funziona")
    
    return False

def test_subitem_extraction_with_syntax(syntax_number):
    """Test estrazione subitem con sintassi corretta"""
    
    if syntax_number == 2:  # items_page con limit
        query = f"""
        query {{
            boards(ids: [{BOARDS['progetti']}]) {{
                items_page(limit: 3) {{
                    items {{
                        id
                        name
                        subitems {{
                            id
                            name
                            column_values {{
                                id
                                text
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
    elif syntax_number == 3:  # Sintassi V1
        query = f"""
        {{
            boards(ids: [{BOARDS['progetti']}]) {{
                items {{
                    id
                    name
                    subitems {{
                        id
                        name
                        column_values {{
                            id
                            text
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
        """
    else:
        return False
    
    logger.info(f"🔍 Test estrazione subitem con sintassi {syntax_number}...")
    
    data = monday_api_call(query)
    
    if not data:
        return False
    
    # Analizza struttura
    try:
        if syntax_number == 2:
            items = data['data']['boards'][0]['items_page']['items']
        else:
            items = data['data']['boards'][0]['items']
        
        subitem_count = 0
        revenue_count = 0
        
        logger.info(f"📊 ANALISI {len(items)} PROGETTI:")
        
        for item in items:
            logger.info(f"Progetto: {item['name']}")
            
            if 'subitems' in item and item['subitems']:
                for subitem in item['subitems']:
                    subitem_count += 1
                    logger.info(f"  Subitem: {subitem['name']}")
                    
                    for col_value in subitem['column_values']:
                        col_type = col_value['column']['type']
                        text_value = col_value.get('text', '')
                        
                        if col_type == 'numbers' and text_value:
                            revenue_count += 1
                            logger.info(f"    💰 Ricavo: €{text_value} (Campo: {col_value['id']})")
        
        logger.info(f"✅ Subitem trovati: {subitem_count}")
        logger.info(f"✅ Con ricavi: {revenue_count}")
        
        # Se funziona, procediamo con estrazione completa
        if subitem_count > 0:
            return extract_all_data_with_correct_syntax(syntax_number)
        
    except Exception as e:
        logger.error(f"Errore analisi: {e}")
    
    return False

def extract_all_data_with_correct_syntax(syntax_number):
    """Estrazione completa con sintassi corretta"""
    
    logger.info("🚀 ESTRAZIONE COMPLETA CON SINTASSI CORRETTA")
    
    # Query per tutti i dati - adatta sintassi
    if syntax_number == 2:  # items_page
        query = f"""
        query {{
            boards(ids: [{BOARDS['progetti']}]) {{
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
    else:  # Sintassi V1
        query = f"""
        {{
            boards(ids: [{BOARDS['progetti']}]) {{
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
        """
    
    # Estrai dati
    data = monday_api_call(query)
    
    if not data:
        logger.error("❌ Estrazione fallita")
        return False
    
    try:
        # Processa dati
        if syntax_number == 2:
            items = data['data']['boards'][0]['items_page']['items']
        else:
            items = data['data']['boards'][0]['items']
        
        extraction_date = datetime.now().date()
        extraction_timestamp = datetime.now()
        
        projects_data = []
        subitems_data = []
        
        for item in items:
            # Progetti
            project_data = {
                'extraction_date': extraction_date,
                'extraction_timestamp': extraction_timestamp,
                'project_id': item['id'],
                'project_name': item['name'],
                'created_at': item.get('created_at'),
                'updated_at': item.get('updated_at'),
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
                elif col_id == 'status_1' and text_value:
                    project_data['circolo'] = text_value
                # Aggiungi altri mapping se necessario
            
            projects_data.append(project_data)
            
            # Subitem
            if 'subitems' in item and item['subitems']:
                for subitem in item['subitems']:
                    subitem_data = {
                        'extraction_date': extraction_date,
                        'extraction_timestamp': extraction_timestamp,
                        'subitem_id': subitem['id'],
                        'project_id': item['id'],
                        'subitem_name': subitem['name'],
                        'created_at': subitem.get('created_at'),
                        'updated_at': subitem.get('updated_at'),
                        'po': None,
                        'timeline_start': None,
                        'timeline_end': None,
                        'revenue_amount': 0,
                        'status': None,
                        'tipologia': None
                    }
                    
                    # Estrai ricavi
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
                    
                    subitems_data.append(subitem_data)
        
        # Carica in BigQuery
        client = bigquery.Client(project=PROJECT_ID)
        
        # Carica progetti
        if projects_data:
            df_projects = pd.DataFrame(projects_data)
            
            # Tabella corrente
            projects_table = f"{PROJECT_ID}.{DATASET_ID}.projects"
            job = client.load_table_from_dataframe(
                df_projects, projects_table, 
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            )
            job.result()
            
            # Tabella storica
            projects_historical_table = f"{PROJECT_ID}.{DATASET_ID}.projects_historical"
            job_hist = client.load_table_from_dataframe(
                df_projects, projects_historical_table,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            )
            job_hist.result()
            
            logger.info(f"✅ Caricati {len(projects_data)} progetti")
        
        # Carica subitem
        if subitems_data:
            df_subitems = pd.DataFrame(subitems_data)
            
            # Tabella corrente
            subitems_table = f"{PROJECT_ID}.{DATASET_ID}.project_subitems"
            job = client.load_table_from_dataframe(
                df_subitems, subitems_table,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            )
            job.result()
            
            # Tabella storica
            subitems_historical_table = f"{PROJECT_ID}.{DATASET_ID}.subitems_historical"
            job_hist = client.load_table_from_dataframe(
                df_subitems, subitems_historical_table,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            )
            job_hist.result()
            
            revenue_subitems = len([s for s in subitems_data if s['revenue_amount'] > 0])
            total_revenue = sum(s['revenue_amount'] for s in subitems_data)
            
            logger.info(f"✅ Caricati {len(subitems_data)} subitem")
            logger.info(f"💰 Con ricavi: {revenue_subitems}")
            logger.info(f"💰 Totale ricavi: €{total_revenue:,.2f}")
        
        return True
        
    except Exception as e:
        logger.error(f"Errore elaborazione: {e}")
        return False

def main():
    """Main function"""
    logger.info("🔧 FIX ETL MONDAY.COM → BIGQUERY")
    logger.info("=" * 50)
    
    if test_correct_board_query():
        logger.info("🎉 ETL COMPLETATO CON SUCCESSO!")
    else:
        logger.error("❌ ETL FALLITO")

if __name__ == "__main__":
    main()
