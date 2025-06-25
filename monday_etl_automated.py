#!/usr/bin/env python3
"""
Sistema ETL Automatizzato Monday.com → BigQuery
- Estrazione giornaliera automatica
- Storicizzazione completa dei dati
- Logging avanzato e error handling
- Confronto e alert su variazioni
"""

import requests
import json
from google.cloud import bigquery
import logging
import pandas as pd
from datetime import datetime, timedelta
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import traceback

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

# Setup logging
def setup_logging():
    log_filename = f"logs/monday_etl_{datetime.now().strftime('%Y%m%d')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

class MondayETL:
    def __init__(self):
        self.logger = setup_logging()
        self.client = bigquery.Client(project=PROJECT_ID)
        self.extraction_date = datetime.now().date()
        self.extraction_timestamp = datetime.now()
        self.stats = {
            'projects': 0,
            'subitems': 0, 
            'personnel_costs': 0,
            'travel_costs': 0,
            'supplier_costs': 0,
            'total_revenue': 0,
            'errors': []
        }
        
    def create_historical_tables_if_not_exist(self):
        """Crea TUTTE le tabelle storiche se non esistono"""
        
        # Schema per tabella storica progetti
        projects_historical_schema = [
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
        
        # Schema per tabella storica subitem
        subitems_historical_schema = [
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
        
        # Schema per tabella storica personnel costs
        personnel_costs_historical_schema = [
            bigquery.SchemaField("extraction_date", "DATE"),
            bigquery.SchemaField("extraction_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("cost_id", "STRING"),
            bigquery.SchemaField("cost_name", "STRING"),
            bigquery.SchemaField("person", "STRING"),
            bigquery.SchemaField("amount", "FLOAT64"),
            bigquery.SchemaField("linked_subitem_id", "STRING"),
            bigquery.SchemaField("linked_subitem_name", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP")
        ]
        
        # Schema per tabella storica travel costs
        travel_costs_historical_schema = [
            bigquery.SchemaField("extraction_date", "DATE"),
            bigquery.SchemaField("extraction_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("cost_id", "STRING"),
            bigquery.SchemaField("cost_name", "STRING"),
            bigquery.SchemaField("person", "STRING"),
            bigquery.SchemaField("amount", "FLOAT64"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("stato", "STRING"),
            bigquery.SchemaField("pagata_con", "STRING"),
            bigquery.SchemaField("linked_subitem_id", "STRING"),
            bigquery.SchemaField("linked_subitem_name", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP")
        ]
        
        # Schema per tabella storica supplier costs
        supplier_costs_historical_schema = [
            bigquery.SchemaField("extraction_date", "DATE"),
            bigquery.SchemaField("extraction_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("cost_id", "STRING"),
            bigquery.SchemaField("cost_name", "STRING"),
            bigquery.SchemaField("imponibile", "FLOAT64"),
            bigquery.SchemaField("tipologia", "STRING"),
            bigquery.SchemaField("stato_ordine", "STRING"),
            bigquery.SchemaField("iva", "FLOAT64"),
            bigquery.SchemaField("linked_subitem_id", "STRING"),
            bigquery.SchemaField("linked_subitem_name", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP")
        ]
        
        # Crea TUTTE le tabelle storiche
        tables_to_create = [
            ("projects_historical", projects_historical_schema),
            ("subitems_historical", subitems_historical_schema),
            ("personnel_costs_historical", personnel_costs_historical_schema),
            ("travel_costs_historical", travel_costs_historical_schema),
            ("supplier_costs_historical", supplier_costs_historical_schema)
        ]
        
        for table_name, schema in tables_to_create:
            table_ref = self.client.dataset(DATASET_ID).table(table_name)
            try:
                self.client.get_table(table_ref)
                self.logger.info(f"✅ Tabella storica {table_name} già esistente")
            except:
                table = bigquery.Table(table_ref, schema=schema)
                # Partiziona per data di estrazione per performance
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="extraction_date"
                )
                table = self.client.create_table(table)
                self.logger.info(f"✅ Creata tabella storica {table_name}")

    def monday_api_call(self, query):
        """Chiamata API Monday.com con retry logic"""
        url = "https://api.monday.com/v2"
        headers = {
            "Authorization": f"Bearer {API_TOKEN}",
            "Content-Type": "application/json"
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.post(url, headers=headers, json={"query": query}, timeout=60)
                response.raise_for_status()
                data = response.json()
                
                if 'errors' in data:
                    raise Exception(f"Monday.com API Error: {data['errors']}")
                    
                return data
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                self.logger.warning(f"⚠️ API call failed, retrying ({attempt + 1}/{max_retries}): {str(e)}")
                
    def extract_projects_and_subitems(self):
        """Estrae progetti e subitem con ricavi"""
        self.logger.info("🚀 Inizio estrazione progetti e subitem...")
        
        query = f"""
        query {{
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
        
        data = self.monday_api_call(query)
        projects_data = []
        subitems_data = []
        
        for item in data['data']['boards'][0]['items']:
            # Estrai dati progetti
            project_data = {
                'extraction_date': self.extraction_date,
                'extraction_timestamp': self.extraction_timestamp,
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
            
            # Estrai subitem
            if item['subitems']:
                for subitem in item['subitems']:
                    subitem_data = {
                        'extraction_date': self.extraction_date,
                        'extraction_timestamp': self.extraction_timestamp,
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
        
        self.stats['projects'] = len(projects_data)
        self.stats['subitems'] = len(subitems_data)
        self.stats['total_revenue'] = sum(s['revenue_amount'] for s in subitems_data)
        
        self.logger.info(f"✅ Estratti {len(projects_data)} progetti e {len(subitems_data)} subitem")
        self.logger.info(f"💰 Ricavi totali: €{self.stats['total_revenue']:,.2f}")
        
        return projects_data, subitems_data
    
    def extract_personnel_costs(self):
        """Estrae costi personale"""
        self.logger.info("🚀 Inizio estrazione costi personale...")
        
        query = f"""
        query {{
            boards(ids: [{BOARDS['personnel']}]) {{
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
                }}
            }}
        }}
        """
        
        data = self.monday_api_call(query)
        personnel_data = []
        
        for item in data['data']['boards'][0]['items']:
            cost_data = {
                'extraction_date': self.extraction_date,
                'extraction_timestamp': self.extraction_timestamp,
                'cost_id': item['id'],
                'cost_name': item['name'],
                'created_at': item.get('created_at'),
                'updated_at': item.get('updated_at'),
                'person': None,
                'amount': 0,
                'linked_subitem_id': None,
                'linked_subitem_name': None
            }
            
            # Mappa campi personnel costs
            for col_value in item['column_values']:
                col_id = col_value['id']
                text_value = col_value.get('text', '')
                
                if col_id == 'person' and text_value:
                    cost_data['person'] = text_value
                elif col_id == 'numbers' and text_value:
                    try:
                        cost_data['amount'] = float(text_value)
                    except:
                        pass
                elif col_id == 'board_relation1' and text_value:
                    # Link to subitem
                    cost_data['linked_subitem_name'] = text_value
                    # Estrai ID se disponibile nel value JSON
                    try:
                        value_data = json.loads(col_value.get('value', '{}'))
                        if 'linkedPulseIds' in value_data and value_data['linkedPulseIds']:
                            cost_data['linked_subitem_id'] = str(value_data['linkedPulseIds'][0]['linkedPulseId'])
                    except:
                        pass
            
            personnel_data.append(cost_data)
        
        self.stats['personnel_costs'] = len(personnel_data)
        self.logger.info(f"✅ Estratti {len(personnel_data)} costi personale")
        
        return personnel_data
    
    def extract_travel_costs(self):
        """Estrae costi trasferte"""
        self.logger.info("🚀 Inizio estrazione costi trasferte...")
        
        query = f"""
        query {{
            boards(ids: [{BOARDS['travel']}]) {{
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
                }}
            }}
        }}
        """
        
        data = self.monday_api_call(query)
        travel_data = []
        
        for item in data['data']['boards'][0]['items']:
            cost_data = {
                'extraction_date': self.extraction_date,
                'extraction_timestamp': self.extraction_timestamp,
                'cost_id': item['id'],
                'cost_name': item['name'],
                'created_at': item.get('created_at'),
                'updated_at': item.get('updated_at'),
                'person': None,
                'amount': 0,
                'date': None,
                'stato': None,
                'pagata_con': None,
                'linked_subitem_id': None,
                'linked_subitem_name': None
            }
            
            # Mappa campi travel costs
            for col_value in item['column_values']:
                col_id = col_value['id']
                text_value = col_value.get('text', '')
                
                if col_id == 'person' and text_value:
                    cost_data['person'] = text_value
                elif col_id == 'numbers' and text_value:
                    try:
                        cost_data['amount'] = float(text_value)
                    except:
                        pass
                elif col_id == 'date' and text_value:
                    try:
                        cost_data['date'] = datetime.strptime(text_value, '%Y-%m-%d').date()
                    except:
                        pass
                elif col_id == 'status' and text_value:
                    cost_data['stato'] = text_value
                elif col_id == 'dropdown' and text_value:
                    cost_data['pagata_con'] = text_value
                elif col_id == 'board_relation39' and text_value:
                    # Link to subitem
                    cost_data['linked_subitem_name'] = text_value
                    try:
                        value_data = json.loads(col_value.get('value', '{}'))
                        if 'linkedPulseIds' in value_data and value_data['linkedPulseIds']:
                            cost_data['linked_subitem_id'] = str(value_data['linkedPulseIds'][0]['linkedPulseId'])
                    except:
                        pass
            
            travel_data.append(cost_data)
        
        self.stats['travel_costs'] = len(travel_data)
        self.logger.info(f"✅ Estratti {len(travel_data)} costi trasferte")
        
        return travel_data
    
    def extract_supplier_costs(self):
        """Estrae costi fornitori"""
        self.logger.info("🚀 Inizio estrazione costi fornitori...")
        
        query = f"""
        query {{
            boards(ids: [{BOARDS['suppliers']}]) {{
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
                }}
            }}
        }}
        """
        
        data = self.monday_api_call(query)
        supplier_data = []
        
        for item in data['data']['boards'][0]['items']:
            cost_data = {
                'extraction_date': self.extraction_date,
                'extraction_timestamp': self.extraction_timestamp,
                'cost_id': item['id'],
                'cost_name': item['name'],
                'created_at': item.get('created_at'),
                'updated_at': item.get('updated_at'),
                'imponibile': 0,
                'tipologia': None,
                'stato_ordine': None,
                'iva': 0,
                'linked_subitem_id': None,
                'linked_subitem_name': None
            }
            
            # Mappa campi supplier costs
            for col_value in item['column_values']:
                col_id = col_value['id']
                text_value = col_value.get('text', '')
                
                if col_id == 'numbers' and text_value:  # Imponibile
                    try:
                        cost_data['imponibile'] = float(text_value)
                    except:
                        pass
                elif col_id == 'numbers8' and text_value:  # IVA
                    try:
                        cost_data['iva'] = float(text_value)
                    except:
                        pass
                elif col_id == 'status' and text_value:
                    cost_data['tipologia'] = text_value
                elif col_id == 'status_1' and text_value:
                    cost_data['stato_ordine'] = text_value
                elif col_id == 'board_relation' and text_value:
                    # Link to subitem
                    cost_data['linked_subitem_name'] = text_value
                    try:
                        value_data = json.loads(col_value.get('value', '{}'))
                        if 'linkedPulseIds' in value_data and value_data['linkedPulseIds']:
                            cost_data['linked_subitem_id'] = str(value_data['linkedPulseIds'][0]['linkedPulseId'])
                    except:
                        pass
            
            supplier_data.append(cost_data)
        
        self.stats['supplier_costs'] = len(supplier_data)
        total_supplier_cost = sum(c['imponibile'] for c in supplier_data)
        self.logger.info(f"✅ Estratti {len(supplier_data)} costi fornitori - Totale: €{total_supplier_cost:,.2f}")
        
        return supplier_data
    
    def load_to_bigquery(self, data, table_name, schema=None):
        """Carica dati in BigQuery con gestione errori"""
        if not data:
            self.logger.warning(f"⚠️ Nessun dato da caricare per {table_name}")
            return
            
        try:
            df = pd.DataFrame(data)
            
            # Carica in tabella corrente (sovrascrive)
            current_table = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                autodetect=True if not schema else False,
                schema=schema if schema else None
            )
            
            job = self.client.load_table_from_dataframe(df, current_table, job_config=job_config)
            job.result()
            
            # Carica anche in tabella storica (append)
            historical_table = f"{PROJECT_ID}.{DATASET_ID}.{table_name}_historical"
            job_config_hist = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                autodetect=True
            )
            
            job_hist = self.client.load_table_from_dataframe(df, historical_table, job_config_hist)
            job_hist.result()
            
            self.logger.info(f"✅ Caricati {len(data)} record in {table_name} (corrente + storico)")
            
        except Exception as e:
            error_msg = f"❌ Errore caricamento {table_name}: {str(e)}"
            self.logger.error(error_msg)
            self.stats['errors'].append(error_msg)
            raise e

    def compare_with_previous_day(self):
        """Confronta dati con il giorno precedente"""
        try:
            yesterday = self.extraction_date - timedelta(days=1)
            
            query = f"""
            WITH today_data AS (
                SELECT 
                    COUNT(*) as projects_today,
                    (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.project_subitems`) as subitems_today,
                    (SELECT ROUND(SUM(revenue_amount), 2) FROM `{PROJECT_ID}.{DATASET_ID}.project_subitems`) as revenue_today
                FROM `{PROJECT_ID}.{DATASET_ID}.projects`
            ),
            yesterday_data AS (
                SELECT 
                    COUNT(*) as projects_yesterday,
                    (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.subitems_historical` WHERE extraction_date = '{yesterday}') as subitems_yesterday,
                    (SELECT ROUND(SUM(revenue_amount), 2) FROM `{PROJECT_ID}.{DATASET_ID}.subitems_historical` WHERE extraction_date = '{yesterday}') as revenue_yesterday
                FROM `{PROJECT_ID}.{DATASET_ID}.projects_historical` 
                WHERE extraction_date = '{yesterday}'
            )
            SELECT 
                *,
                projects_today - projects_yesterday as projects_diff,
                subitems_today - subitems_yesterday as subitems_diff,
                revenue_today - revenue_yesterday as revenue_diff
            FROM today_data, yesterday_data
            """
            
            results = self.client.query(query).to_dataframe()
            
            if not results.empty:
                row = results.iloc[0]
                self.logger.info("📊 CONFRONTO CON IERI:")
                self.logger.info(f"   Progetti: {row['projects_today']} ({row['projects_diff']:+d})")
                self.logger.info(f"   Subitem: {row['subitems_today']} ({row['subitems_diff']:+d})")
                self.logger.info(f"   Ricavi: €{row['revenue_today']:,.2f} (€{row['revenue_diff']:+,.2f})")
                
                return {
                    'projects_diff': row['projects_diff'],
                    'subitems_diff': row['subitems_diff'], 
                    'revenue_diff': row['revenue_diff']
                }
        except Exception as e:
            self.logger.warning(f"⚠️ Impossibile confrontare con ieri: {str(e)}")
            return None

    def send_notification(self, comparison_data=None):
        """Invia notifica email con risultati ETL"""
        try:
            # Configura email (adatta alle tue impostazioni SMTP)
            subject = f"ETL Monday.com → BigQuery - {self.extraction_date}"
            
            body = f"""
📊 REPORT ETL GIORNALIERO - {self.extraction_date}

✅ DATI ESTRATTI:
• Progetti: {self.stats['projects']}
• Subitem: {self.stats['subitems']} 
• Ricavi totali: €{self.stats['total_revenue']:,.2f}

"""
            
            if comparison_data:
                body += f"""
📈 VARIAZIONI DA IERI:
• Progetti: {comparison_data['projects_diff']:+d}
• Subitem: {comparison_data['subitems_diff']:+d}
• Ricavi: €{comparison_data['revenue_diff']:+,.2f}

"""
            
            if self.stats['errors']:
                body += f"""
❌ ERRORI:
{chr(10).join(self.stats['errors'])}
"""
            else:
                body += "✅ Nessun errore\n"
                
            body += f"""
🕐 Completato alle: {datetime.now().strftime('%H:%M:%S')}

Dashboard: https://lookerstudio.google.com/your-dashboard-url
BigQuery: https://console.cloud.google.com/bigquery?project={PROJECT_ID}
"""
            
            self.logger.info("📧 Notifica preparata (configurare SMTP per invio)")
            self.logger.info(f"Soggetto: {subject}")
            
        except Exception as e:
            self.logger.error(f"❌ Errore preparazione notifica: {str(e)}")

    def run_daily_etl(self):
        """Esegue ETL completo giornaliero per TUTTE le tabelle"""
        start_time = datetime.now()
        self.logger.info(f"🚀 INIZIO ETL COMPLETO GIORNALIERO - {self.extraction_date}")
        
        try:
            # 1. Crea tabelle storiche se necessario
            self.create_historical_tables_if_not_exist()
            
            # 2. Estrai TUTTI i dati
            self.logger.info("📥 ESTRAZIONE DATI DA MONDAY.COM...")
            
            # 2a. Progetti e subitem
            projects_data, subitems_data = self.extract_projects_and_subitems()
            
            # 2b. Costi personale
            personnel_data = self.extract_personnel_costs()
            
            # 2c. Costi trasferte
            travel_data = self.extract_travel_costs()
            
            # 2d. Costi fornitori
            supplier_data = self.extract_supplier_costs()
            
            # 3. Carica TUTTO in BigQuery
            self.logger.info("📤 CARICAMENTO IN BIGQUERY...")
            
            self.load_to_bigquery(projects_data, "projects")
            self.load_to_bigquery(subitems_data, "project_subitems")
            self.load_to_bigquery(personnel_data, "personnel_costs")
            self.load_to_bigquery(travel_data, "travel_costs")
            self.load_to_bigquery(supplier_data, "supplier_costs")
            
            # 4. Confronta con ieri
            comparison_data = self.compare_with_previous_day()
            
            # 5. Log riassunto completo
            self.logger.info("📊 RIASSUNTO ESTRAZIONE COMPLETA:")
            self.logger.info(f"   Progetti: {self.stats['projects']}")
            self.logger.info(f"   Subitem: {self.stats['subitems']} (Ricavi: €{self.stats['total_revenue']:,.2f})")
            self.logger.info(f"   Costi personale: {self.stats['personnel_costs']}")
            self.logger.info(f"   Costi trasferte: {self.stats['travel_costs']}")
            self.logger.info(f"   Costi fornitori: {self.stats['supplier_costs']}")
            
            # 6. Invia notifica
            self.send_notification(comparison_data)
            
            duration = datetime.now() - start_time
            self.logger.info(f"🎉 ETL COMPLETO COMPLETATO in {duration.total_seconds():.1f} secondi")
            
            return True
            
        except Exception as e:
            error_msg = f"💥 ETL COMPLETO FALLITO: {str(e)}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            self.stats['errors'].append(error_msg)
            
            # Invia notifica di errore
            self.send_notification()
            
            return False

def main():
    """Funzione principale"""
    etl = MondayETL()
    success = etl.run_daily_etl()
    
    if success:
        print("✅ ETL completato con successo")
        exit(0)
    else:
        print("❌ ETL fallito")
        exit(1)

if __name__ == "__main__":
    main()
