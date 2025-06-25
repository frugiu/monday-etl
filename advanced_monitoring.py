#!/usr/bin/env python3
"""
Sistema di Monitoraggio Avanzato ETL Monday.com → BigQuery
- Dashboard di monitoraggio
- Alert automatici
- Analisi trend storici
- Health check sistema
"""

import pandas as pd
from google.cloud import bigquery
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import logging
import json
import requests
from dataclasses import dataclass
from typing import List, Dict, Optional
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

@dataclass
class ETLMetrics:
    """Metriche ETL"""
    date: str
    projects: int
    subitems: int
    revenue: float
    extraction_time: Optional[str] = None
    success: bool = True
    errors: List[str] = None

class ETLMonitor:
    def __init__(self, project_id="monday-reporting-1750794511", dataset_id="monday_data"):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.client = bigquery.Client(project=project_id)
        self.logger = self._setup_logging()
        
    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def get_daily_metrics(self, days_back: int = 30) -> pd.DataFrame:
        """Ottiene metriche giornaliere degli ultimi N giorni - FIX AMBIGUITÀ"""
        query = f"""
        WITH daily_metrics AS (
          SELECT 
            p.extraction_date,
            COUNT(DISTINCT p.project_id) as projects,
            COUNT(s.subitem_id) as subitems,
            COUNT(CASE WHEN s.revenue_amount > 0 THEN s.subitem_id END) as subitems_with_revenue,
            ROUND(SUM(s.revenue_amount), 2) as total_revenue,
            ROUND(AVG(s.revenue_amount), 2) as avg_revenue,
            MAX(s.extraction_timestamp) as extraction_time
          FROM `{self.project_id}.{self.dataset_id}.projects_historical` p
          LEFT JOIN `{self.project_id}.{self.dataset_id}.subitems_historical` s
            ON p.project_id = s.project_id AND p.extraction_date = s.extraction_date
          WHERE p.extraction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
          GROUP BY p.extraction_date
          ORDER BY p.extraction_date DESC
        )
        SELECT 
          *,
          total_revenue - LAG(total_revenue) OVER (ORDER BY extraction_date) as revenue_change,
          subitems - LAG(subitems) OVER (ORDER BY extraction_date) as subitems_change
        FROM daily_metrics
        """
        
        try:
            return self.client.query(query).to_dataframe()
        except Exception as e:
            self.logger.error(f"Errore recupero metriche: {e}")
            return pd.DataFrame()
    
    def get_circle_performance(self, days_back: int = 7) -> pd.DataFrame:
        """Analisi performance per Circle - FIX AMBIGUITÀ"""
        query = f"""
        SELECT 
          p.circolo,
          p.extraction_date,
          COUNT(DISTINCT p.project_id) as projects,
          COUNT(s.subitem_id) as subitems,
          ROUND(SUM(s.revenue_amount), 2) as revenue,
          ROUND(AVG(s.revenue_amount), 2) as avg_revenue_per_subitem
        FROM `{self.project_id}.{self.dataset_id}.projects_historical` p
        LEFT JOIN `{self.project_id}.{self.dataset_id}.subitems_historical` s
          ON p.project_id = s.project_id AND p.extraction_date = s.extraction_date
        WHERE p.extraction_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
          AND p.circolo IN ('Radical', 'WoW', 'GCC', 'BDTC')
        GROUP BY p.circolo, p.extraction_date
        ORDER BY p.extraction_date DESC, revenue DESC
        """
        
        try:
            return self.client.query(query).to_dataframe()
        except Exception as e:
            self.logger.error(f"Errore recupero performance Circle: {e}")
            return pd.DataFrame()
    
    def check_data_quality(self) -> Dict:
        """Verifica qualità dati per TUTTE le tabelle"""
        checks = {}
        
        # 1. Verifica completezza dati oggi - TUTTE LE TABELLE
        today_query = f"""
        SELECT 
          (SELECT COUNT(*) FROM `{self.project_id}.{self.dataset_id}.projects`) as projects_today,
          (SELECT COUNT(*) FROM `{self.project_id}.{self.dataset_id}.project_subitems`) as subitems_today,
          (SELECT COUNT(*) FROM `{self.project_id}.{self.dataset_id}.project_subitems` WHERE revenue_amount > 0) as revenue_subitems_today,
          (SELECT ROUND(SUM(revenue_amount), 2) FROM `{self.project_id}.{self.dataset_id}.project_subitems`) as total_revenue_today,
          (SELECT COUNT(*) FROM `{self.project_id}.{self.dataset_id}.personnel_costs`) as personnel_costs_today,
          (SELECT ROUND(SUM(amount), 2) FROM `{self.project_id}.{self.dataset_id}.personnel_costs`) as total_personnel_cost_today,
          (SELECT COUNT(*) FROM `{self.project_id}.{self.dataset_id}.travel_costs`) as travel_costs_today,
          (SELECT ROUND(SUM(amount), 2) FROM `{self.project_id}.{self.dataset_id}.travel_costs`) as total_travel_cost_today,
          (SELECT COUNT(*) FROM `{self.project_id}.{self.dataset_id}.supplier_costs`) as supplier_costs_today,
          (SELECT ROUND(SUM(imponibile), 2) FROM `{self.project_id}.{self.dataset_id}.supplier_costs`) as total_supplier_cost_today
        """
        
        try:
            today_data = self.client.query(today_query).to_dataframe().iloc[0]
            checks['data_completeness'] = {
                'projects': int(today_data['projects_today']),
                'subitems': int(today_data['subitems_today']),
                'revenue_subitems': int(today_data['revenue_subitems_today']),
                'total_revenue': float(today_data['total_revenue_today']) if pd.notna(today_data['total_revenue_today']) else 0,
                'personnel_costs': int(today_data['personnel_costs_today']),
                'total_personnel_cost': float(today_data['total_personnel_cost_today']) if pd.notna(today_data['total_personnel_cost_today']) else 0,
                'travel_costs': int(today_data['travel_costs_today']),
                'total_travel_cost': float(today_data['total_travel_cost_today']) if pd.notna(today_data['total_travel_cost_today']) else 0,
                'supplier_costs': int(today_data['supplier_costs_today']),
                'total_supplier_cost': float(today_data['total_supplier_cost_today']) if pd.notna(today_data['total_supplier_cost_today']) else 0,
                'revenue_coverage': round(today_data['revenue_subitems_today'] / today_data['subitems_today'] * 100, 1) if today_data['subitems_today'] > 0 else 0
            }
        except Exception as e:
            checks['data_completeness'] = {'error': str(e)}
        
        # 2. Verifica duplicati
        duplicates_query = f"""
        SELECT COUNT(*) as duplicates
        FROM (
          SELECT subitem_id, COUNT(*) as cnt
          FROM `{self.project_id}.{self.dataset_id}.project_subitems`
          GROUP BY subitem_id
          HAVING cnt > 1
        )
        """
        
        try:
            duplicates = self.client.query(duplicates_query).to_dataframe().iloc[0]['duplicates']
            checks['duplicates'] = int(duplicates)
        except Exception as e:
            checks['duplicates'] = {'error': str(e)}
        
        # 3. Verifica dati recenti
        freshness_query = f"""
        SELECT 
          MAX(extraction_date) as last_extraction,
          DATE_DIFF(CURRENT_DATE(), MAX(extraction_date), DAY) as days_since_last_extraction
        FROM `{self.project_id}.{self.dataset_id}.projects_historical`
        """
        
        try:
            freshness = self.client.query(freshness_query).to_dataframe().iloc[0]
            checks['data_freshness'] = {
                'last_extraction': str(freshness['last_extraction']),
                'days_since': int(freshness['days_since_last_extraction'])
            }
        except Exception as e:
            checks['data_freshness'] = {'error': str(e)}
        
        return checks
    
    def get_current_circle_performance(self) -> pd.DataFrame:
        """Performance Circle corrente (senza join storiche per evitare ambiguità)"""
        query = f"""
        SELECT 
          p.circolo,
          COUNT(DISTINCT p.project_id) as projects,
          COUNT(s.subitem_id) as subitems,
          ROUND(SUM(s.revenue_amount), 2) as revenue,
          ROUND(AVG(s.revenue_amount), 2) as avg_revenue_per_subitem
        FROM `{self.project_id}.{self.dataset_id}.projects` p
        LEFT JOIN `{self.project_id}.{self.dataset_id}.project_subitems` s
          ON p.project_id = s.project_id
        WHERE p.circolo IN ('Radical', 'WoW', 'GCC', 'BDTC')
        GROUP BY p.circolo
        ORDER BY revenue DESC
        """
        
        try:
            return self.client.query(query).to_dataframe()
        except Exception as e:
            self.logger.error(f"Errore recupero performance Circle corrente: {e}")
            return pd.DataFrame()
    
    def generate_health_report(self) -> str:
        """Genera report di salute del sistema"""
        report = ["🔍 HEALTH CHECK ETL MONDAY.COM → BIGQUERY"]
        report.append("=" * 50)
        report.append(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Metriche giornaliere (solo se disponibili)
        daily_metrics = self.get_daily_metrics(7)
        if not daily_metrics.empty:
            latest = daily_metrics.iloc[0]
            report.append("📊 METRICHE CORRENTI:")
            report.append(f"   Progetti: {latest['projects']}")
            report.append(f"   Subitem: {latest['subitems']}")
            report.append(f"   Ricavi: €{latest['total_revenue']:,.2f}")
            report.append(f"   Ultima estrazione: {latest['extraction_date']}")
            
            if len(daily_metrics) > 1:
                prev = daily_metrics.iloc[1]
                report.append("")
                report.append("📈 VARIAZIONI DA IERI:")
                revenue_change = latest['total_revenue'] - prev['total_revenue'] if pd.notna(prev['total_revenue']) else 0
                subitems_change = latest['subitems'] - prev['subitems']
                report.append(f"   Ricavi: €{revenue_change:+,.2f}")
                report.append(f"   Subitem: {subitems_change:+d}")
        
        # Qualità dati - TUTTE LE TABELLE
        quality = self.check_data_quality()
        report.append("")
        report.append("🔍 QUALITÀ DATI - COMPLETE:")
        
        if 'data_completeness' in quality and 'error' not in quality['data_completeness']:
            comp = quality['data_completeness']
            report.append(f"   📊 PROGETTI: {comp['projects']}")
            report.append(f"   📊 SUBITEM: {comp['subitems']} (Ricavi: {comp['revenue_subitems']}, Copertura: {comp['revenue_coverage']}%)")
            report.append(f"   💰 RICAVI TOTALI: €{comp['total_revenue']:,.2f}")
            report.append(f"   👥 COSTI PERSONALE: {comp['personnel_costs']} (€{comp['total_personnel_cost']:,.2f})")
            report.append(f"   ✈️ COSTI TRASFERTE: {comp['travel_costs']} (€{comp['total_travel_cost']:,.2f})")
            report.append(f"   🏭 COSTI FORNITORI: {comp['supplier_costs']} (€{comp['total_supplier_cost']:,.2f})")
            
            # Calcola P&L rapido
            total_costs = comp['total_personnel_cost'] + comp['total_travel_cost'] + comp['total_supplier_cost']
            net_margin = comp['total_revenue'] - total_costs
            margin_pct = (net_margin / comp['total_revenue'] * 100) if comp['total_revenue'] > 0 else 0
            report.append(f"   💹 MARGINE NETTO: €{net_margin:,.2f} ({margin_pct:.1f}%)")
        
        if 'duplicates' in quality and isinstance(quality['duplicates'], int):
            status = "✅ OK" if quality['duplicates'] == 0 else f"⚠️ {quality['duplicates']} duplicati"
            report.append(f"   Duplicati: {status}")
        
        if 'data_freshness' in quality and 'error' not in quality['data_freshness']:
            fresh = quality['data_freshness']
            status = "✅ Aggiornato" if fresh['days_since'] == 0 else f"⚠️ {fresh['days_since']} giorni fa"
            report.append(f"   Freschezza dati: {status}")
        
        # Performance Circle corrente
        circle_perf = self.get_current_circle_performance()
        if not circle_perf.empty:
            report.append("")
            report.append("🎯 PERFORMANCE CIRCLE (CORRENTE):")
            for _, row in circle_perf.iterrows():
                if pd.notna(row['revenue']) and row['revenue'] > 0:
                    report.append(f"   {row['circolo']}: €{row['revenue']:,.2f} ({row['subitems']} subitem)")
        
        return "\n".join(report)
    
    def create_trend_charts(self, save_path: str = "etl_trends.png"):
        """Crea grafici trend per il monitoraggio"""
        daily_metrics = self.get_daily_metrics(30)
        
        if daily_metrics.empty:
            self.logger.warning("Nessun dato storico per creare grafici")
            return None
        
        # Setup grafici
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('ETL Monday.com → BigQuery - Trend Ultimi 30 Giorni', fontsize=16)
        
        # Converti date
        daily_metrics['extraction_date'] = pd.to_datetime(daily_metrics['extraction_date'])
        daily_metrics = daily_metrics.sort_values('extraction_date')
        
        # 1. Trend ricavi
        ax1.plot(daily_metrics['extraction_date'], daily_metrics['total_revenue'], 'b-', linewidth=2)
        ax1.set_title('Ricavi Totali')
        ax1.set_ylabel('Euro (€)')
        ax1.tick_params(axis='x', rotation=45)
        ax1.grid(True, alpha=0.3)
        
        # 2. Trend subitem
        ax2.plot(daily_metrics['extraction_date'], daily_metrics['subitems'], 'g-', linewidth=2)
        ax2.plot(daily_metrics['extraction_date'], daily_metrics['subitems_with_revenue'], 'r--', linewidth=2)
        ax2.set_title('Subitem Totali vs Con Ricavi')
        ax2.legend(['Totali', 'Con Ricavi'])
        ax2.set_ylabel('Numero Subitem')
        ax2.tick_params(axis='x', rotation=45)
        ax2.grid(True, alpha=0.3)
        
        # 3. Variazioni giornaliere ricavi
        revenue_changes = daily_metrics['revenue_change'].dropna()
        dates_changes = daily_metrics[daily_metrics['revenue_change'].notna()]['extraction_date']
        colors = ['green' if x >= 0 else 'red' for x in revenue_changes]
        ax3.bar(dates_changes, revenue_changes, color=colors, alpha=0.7)
        ax3.set_title('Variazioni Giornaliere Ricavi')
        ax3.set_ylabel('Variazione Euro (€)')
        ax3.tick_params(axis='x', rotation=45)
        ax3.axhline(y=0, color='black', linestyle='-', alpha=0.3)
        ax3.grid(True, alpha=0.3)
        
        # 4. Ricavo medio per subitem
        ax4.plot(daily_metrics['extraction_date'], daily_metrics['avg_revenue'], 'purple', linewidth=2)
        ax4.set_title('Ricavo Medio per Subitem')
        ax4.set_ylabel('Euro (€)')
        ax4.tick_params(axis='x', rotation=45)
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        self.logger.info(f"Grafici salvati in: {save_path}")
        return save_path
    
    def send_monitoring_email(self, recipients: List[str], smtp_config: Dict):
        """Invia email di monitoraggio con report e grafici"""
        try:
            # Genera report
            health_report = self.generate_health_report()
            
            # Crea grafici
            chart_path = self.create_trend_charts()
            
            # Setup email
            msg = MIMEMultipart()
            msg['From'] = smtp_config['username']
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = f"ETL Monitoring Report - {datetime.now().strftime('%Y-%m-%d')}"
            
            # Corpo email
            body = f"""
{health_report}

---
Dashboard BigQuery: https://console.cloud.google.com/bigquery?project={self.project_id}
Generato automaticamente dal sistema di monitoraggio ETL
            """
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Allega grafico se disponibile
            if chart_path:
                with open(chart_path, "rb") as attachment:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        'Content-Disposition',
                        f'attachment; filename= etl_trends_{datetime.now().strftime("%Y%m%d")}.png'
                    )
                    msg.attach(part)
            
            # Invia email
            server = smtplib.SMTP(smtp_config['smtp_server'], smtp_config['smtp_port'])
            server.starttls()
            server.login(smtp_config['username'], smtp_config['password'])
            server.send_message(msg)
            server.quit()
            
            self.logger.info(f"Report inviato a: {', '.join(recipients)}")
            
        except Exception as e:
            self.logger.error(f"Errore invio email: {e}")
    
    def check_alerts(self) -> List[str]:
        """Verifica condizioni di alert"""
        alerts = []
        
        # Verifica dati freschi
        quality = self.check_data_quality()
        if 'data_freshness' in quality and 'days_since' in quality['data_freshness']:
            if quality['data_freshness']['days_since'] > 1:
                alerts.append(f"⚠️ Dati non aggiornati da {quality['data_freshness']['days_since']} giorni")
        
        # Verifica copertura ricavi
        if 'data_completeness' in quality and 'revenue_coverage' in quality['data_completeness']:
            coverage = quality['data_completeness']['revenue_coverage']
            if coverage < 60:  # Soglia minima 60%
                alerts.append(f"⚠️ Copertura ricavi bassa: {coverage}%")
        
        # Verifica duplicati
        if 'duplicates' in quality and isinstance(quality['duplicates'], int):
            if quality['duplicates'] > 0:
                alerts.append(f"⚠️ Trovati {quality['duplicates']} record duplicati")
        
        # Verifica trend negativo ricavi (solo se ci sono dati storici)
        daily_metrics = self.get_daily_metrics(7)
        if len(daily_metrics) >= 2:
            latest_revenue = daily_metrics.iloc[0]['total_revenue']
            week_ago_revenue = daily_metrics.iloc[-1]['total_revenue']
            if latest_revenue < week_ago_revenue * 0.95:  # Calo > 5%
                change_pct = ((latest_revenue - week_ago_revenue) / week_ago_revenue) * 100
                alerts.append(f"📉 Calo ricavi settimanale: {change_pct:.1f}%")
        
        return alerts

def main():
    """Funzione principale per monitoraggio"""
    monitor = ETLMonitor()
    
    # Genera e stampa report
    print(monitor.generate_health_report())
    
    # Verifica alert
    alerts = monitor.check_alerts()
    if alerts:
        print("\n🚨 ALERT:")
        for alert in alerts:
            print(f"   {alert}")
    else:
        print("\n✅ Nessun alert attivo")
    
    # Crea grafici se richiesto
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == '--charts':
        chart_path = monitor.create_trend_charts()
        print(f"\n📊 Grafici creati: {chart_path}")

if __name__ == "__main__":
    main()
