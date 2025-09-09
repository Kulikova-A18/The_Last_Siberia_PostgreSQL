import typer
import json as json_lib
from rich.console import Console
from .models import T1CloudService, AnalysisReport, Priority
from .analyzer import T1PgQueryAnalyzer
from typing import Optional
from .pdf_report import generate_pdf_report
import os
from datetime import datetime

console = Console()

def print_detailed_report(report: AnalysisReport, max_cost: float):
    pass

app = typer.Typer(name="The_Last_Siberia", help="SQL Query Analyzer for T1 Cloud")

DEFAULT_DSN = '''
psql "host=193.246.150.20 port=5000 sslmode=require dbname=The_Last_Siberia user=kagamine01 password=Russia123- target_session_attrs=read-write"
'''

@app.command()
def analyze(
        dsn: str = typer.Argument(DEFAULT_DSN.strip(), help="PostgreSQL DSN для T1 Cloud (поддерживается формат psql \"host=...\")"),
        query: Optional[str] = typer.Option(None, "--query", "-q", help="SQL запрос для анализа"),
        file: Optional[str] = typer.Option(None, "--file", "-f", help="Файл с SQL запросами"),
        max_cost: float = typer.Option(5000.0, "--max-cost", help="Максимально допустимая стоимость"),
        t1_env: Optional[str] = typer.Option("demo", "--t1-env", help="Окружение T1 Cloud (prod/stage/test)"),
        output: str = typer.Option("text", "--output", "-o", help="Формат вывода (text/json)"),
        verbose: bool = typer.Option(False, "--verbose", "-v", help="Подробный вывод")
):
    analyzer = T1PgQueryAnalyzer(dsn, t1_env, verbose)

    try:
        analyzer.connect()

        if query:
            queries = [query]
        elif file:
            with open(file, 'r', encoding='utf-8') as f:
                queries = [f.read()]
        else:
            raise typer.Exit(1)

        reports = []
        for i, q in enumerate(queries):
            report = analyzer.analyze_query(q)
            reports.append(report)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            pdf_filename = f"reports/report_{timestamp}.pdf"
            generate_pdf_report(report, pdf_filename)

            if report.is_critical or report.metrics.total_cost > max_cost:
                raise typer.Exit(1)

    except Exception:
        raise typer.Exit(1)

@app.command()
def list_services():
    pass

@app.command()
def version():
    pass