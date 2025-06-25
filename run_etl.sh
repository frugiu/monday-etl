#!/bin/bash

# ETL Script per macOS
echo "🚀 Avvio ETL Monday.com → BigQuery"
echo "Timestamp: $(date)"

# Imposta directory di lavoro
cd ~/monday-etl

# Esegui ETL con logging
python3 etl_final_fix.py 2>&1 | tee logs/etl_$(date +%Y%m%d_%H%M%S).log

# Mostra risultato
if [ $? -eq 0 ]; then
    echo "✅ ETL completato con successo"
    echo "$(date): ETL completato con successo" >> logs/etl_status.log
else
    echo "❌ ETL fallito"
    echo "$(date): ETL fallito" >> logs/etl_status.log
fi
