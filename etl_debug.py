#!/usr/bin/env python3
"""
ETL Monday.com → BigQuery - Versione Debug
Estrazione semplificata per diagnosticare problemi API
"""

import requests
import json
from google.cloud import bigquery
import logging
from datetime import datetime

# Configurazione
API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJ0aWQiOjIzNDU4MjgxNiwiYWFpIjoxMSwidWlkIjozNzYxNDgwNSwiaWFkIjoiMjAyMy0wMi0wNFQxNjo0MTo0Ny4wMDBaIiwicGVyIjoibWU6d3JpdGUiLCJhY3RpZCI6MTQ1Nzk3NjYsInJnbiI6InVzZTEifQ.Baci-5D9rHkOGh9LoFHpxCM5spTHm4TJ1PekdC8Uk9c"
PROJECT_ID = "monday-reporting-1750794511"
DATASET_ID = "monday_data"
BOARD_ID = "8113598675"

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_api_simple():
    """Test semplice API Monday.com"""
    url = "https://api.monday.com/v2"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Query molto semplice per test
    simple_query = """
    query {
        me {
            name
            email
        }
    }
    """
    
    try:
        response = requests.post(url, headers=headers, json={"query": simple_query})
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"Errore API test: {e}")
        return False

def test_board_access():
    """Test accesso alla board progetti"""
    url = "https://api.monday.com/v2"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Query semplice per la board
    board_query = f"""
    query {{
        boards(ids: [{BOARD_ID}]) {{
            id
            name
            items(limit: 5) {{
                id
                name
            }}
        }}
    }}
    """
    
    try:
        response = requests.post(url, headers=headers, json={"query": board_query})
        print(f"Status Code: {response.status_code}")
        data = response.json()
        
        if 'errors' in data:
            print(f"Errori API: {data['errors']}")
            return False
        
        board = data['data']['boards'][0]
        print(f"Board: {board['name']} (ID: {board['id']})")
        print(f"Primi 5 progetti: {len(board['items'])} trovati")
        
        for item in board['items']:
            print(f"  - {item['name']} (ID: {item['id']})")
        
        return True
        
    except Exception as e:
        print(f"Errore test board: {e}")
        return False

def test_subitem_extraction():
    """Test estrazione subitem semplificata"""
    url = "https://api.monday.com/v2"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Query per subitem - versione semplificata
    subitem_query = f"""
    query {{
        boards(ids: [{BOARD_ID}]) {{
            items(limit: 3) {{
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
    
    try:
        response = requests.post(url, headers=headers, json={"query": subitem_query})
        print(f"Status Code: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Response Error: {response.text}")
            return False
        
        data = response.json()
        
        if 'errors' in data:
            print(f"Errori API: {data['errors']}")
            return False
        
        # Analizza risposta
        items = data['data']['boards'][0]['items']
        subitem_count = 0
        revenue_count = 0
        
        print(f"\n📊 ANALISI SUBITEM:")
        for item in items:
            print(f"\nProgetto: {item['name']}")
            
            if item['subitems']:
                for subitem in item['subitems']:
                    subitem_count += 1
                    print(f"  Subitem: {subitem['name']}")
                    
                    for col_value in subitem['column_values']:
                        col_type = col_value['column']['type']
                        text_value = col_value.get('text', '')
                        
                        if col_type == 'numbers' and text_value:
                            revenue_count += 1
                            print(f"    💰 Campo numbers: {text_value} (ID: {col_value['id']})")
        
        print(f"\n✅ Totale subitem testati: {subitem_count}")
        print(f"✅ Subitem con numeri: {revenue_count}")
        
        return True
        
    except Exception as e:
        print(f"Errore test subitem: {e}")
        return False

def fix_and_reload_data():
    """Corregge ed ricarica i dati usando il metodo funzionante"""
    from google.cloud import bigquery
    import pandas as pd
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # Se l'API funziona, proviamo ad aggiornare solo i subitem con il metodo che sappiamo funziona
    url = "https://api.monday.com/v2"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Query che sappiamo funziona (dai test precedenti)
    working_query = f"""
    query {{
        boards(ids: [{BOARD_ID}]) {{
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
                            type
                        }}
                    }}
                }}
            }}
        }}
    }}
    """
    
    try:
        print("🔄 Tentativo estrazione con query semplificata...")
        response = requests.post(url, headers=headers, json={"query": working_query})
        
        if response.status_code == 200:
            data = response.json()
            
            if 'errors' not in data:
                # Processa i dati
                subitems_data = []
                extraction_date = datetime.now().date()
                extraction_timestamp = datetime.now()
                
                for item in data['data']['boards'][0]['items']:
                    if item['subitems']:
                        for subitem in item['subitems']:
                            subitem_record = {
                                'extraction_date': extraction_date,
                                'extraction_timestamp': extraction_timestamp,
                                'subitem_id': subitem['id'],
                                'project_id': item['id'],
                                'subitem_name': subitem['name'],
                                'revenue_amount': 0,
                                'po': None,
                                'timeline_start': None,
                                'timeline_end': None,
                                'status': None,
                                'tipologia': None,
                                'created_at': None,
                                'updated_at': None
                            }
                            
                            # Estrai ricavi
                            for col_value in subitem['column_values']:
                                if col_value['column']['type'] == 'numbers' and col_value.get('text'):
                                    try:
                                        subitem_record['revenue_amount'] = float(col_value['text'])
                                        break
                                    except:
                                        pass
                            
                            subitems_data.append(subitem_record)
                
                # Carica in tabella storica
                if subitems_data:
                    df = pd.DataFrame(subitems_data)
                    
                    # Carica in tabella storica
                    historical_table = f"{PROJECT_ID}.{DATASET_ID}.subitems_historical"
                    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                    
                    job = client.load_table_from_dataframe(df, historical_table, job_config=job_config)
                    job.result()
                    
                    revenue_subitems = len([s for s in subitems_data if s['revenue_amount'] > 0])
                    total_revenue = sum(s['revenue_amount'] for s in subitems_data)
                    
                    print(f"✅ Caricati {len(subitems_data)} subitem in tabella storica")
                    print(f"💰 Subitem con ricavi: {revenue_subitems}")
                    print(f"💰 Ricavi totali: €{total_revenue:,.2f}")
                    
                    return True
            else:
                print(f"Errori API: {data['errors']}")
        else:
            print(f"Errore HTTP: {response.status_code}")
            
    except Exception as e:
        print(f"Errore fix: {e}")
        
    return False

def main():
    """Test diagnostico completo"""
    print("🔍 DIAGNOSI ETL MONDAY.COM → BIGQUERY")
    print("=" * 50)
    
    # Test 1: API base
    print("\n1️⃣ Test API base...")
    if test_api_simple():
        print("✅ API Monday.com funzionante")
    else:
        print("❌ API Monday.com non accessibile")
        return
    
    # Test 2: Accesso board
    print("\n2️⃣ Test accesso board progetti...")
    if test_board_access():
        print("✅ Board progetti accessibile")
    else:
        print("❌ Board progetti non accessibile")
        return
    
    # Test 3: Estrazione subitem
    print("\n3️⃣ Test estrazione subitem...")
    if test_subitem_extraction():
        print("✅ Estrazione subitem funzionante")
    else:
        print("❌ Estrazione subitem fallita")
        return
    
    # Test 4: Fix e caricamento
    print("\n4️⃣ Tentativo fix e caricamento...")
    if fix_and_reload_data():
        print("✅ Fix applicato con successo")
    else:
        print("❌ Fix fallito")
    
    print("\n🎉 Diagnosi completata!")

if __name__ == "__main__":
    main()
