import os
from datetime import datetime
from weasyprint import HTML
from .models import AnalysisReport

def generate_pdf_report(report: AnalysisReport, output_path: str = "report.pdf"):
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>PGQueryGuard Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            h1, h2, h3 {{ color: #2c3e50; }}
            .header {{ background-color: #3498db; color: white; padding: 10px; border-radius: 5px; }}
            .section {{ margin: 20px 0; padding: 15px; border: 1px solid #bdc3c7; border-radius: 5px; }}
            .metric-row {{ display: flex; justify-content: space-between; margin: 5px 0; }}
            .metric-label {{ font-weight: bold; color: #34495e; }}
            .recommendation {{ margin: 15px 0; padding: 10px; border-left: 5px solid #e74c3c; background-color: #fdf2f2; }}
            .recommendation.medium {{ border-left-color: #f39c12; background-color: #fef9e7; }}
            .recommendation.low {{ border-left-color: #27ae60; background-color: #e8f8f5; }}
            .warning {{ color: #e67e22; font-weight: bold; }}
            .score-high {{ color: #27ae60; font-weight: bold; }}
            .score-medium {{ color: #f39c12; font-weight: bold; }}
            .score-low {{ color: #e74c3c; font-weight: bold; }}
            pre {{ background-color: #f5f5f5; padding: 10px; border-radius: 5px; overflow-x: auto; }}
            .footer {{ margin-top: 40px; font-size: 0.9em; color: #7f8c8d; text-align: center; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>PGQueryGuard — Анализ SQL-запроса</h1>
            <p><strong>Дата анализа:</strong> {report.analyzed_at.strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>Окружение T1 Cloud:</strong> {report.t1_environment or 'N/A'}</p>
        </div>

        <div class="section">
            <h2>Информация о запросе</h2>
            <div class="metric-row"><span class="metric-label">Тип запроса:</span> <span>{report.query_type}</span></div>
            <div class="metric-row"><span class="metric-label">Затронутые таблицы:</span> <span>{', '.join(report.tables_affected) if report.tables_affected else 'Нет'}</span></div>
            <div class="metric-row"><span class="metric-label">Используемые индексы:</span> <span>{', '.join(report.indexes_used) if report.indexes_used else 'Нет'}</span></div>
        </div>

        <div class="section">
            <h2>Метрики производительности</h2>
            <div class="metric-row"><span class="metric-label">Общая стоимость:</span> <span>{report.metrics.total_cost:.2f}</span></div>
            <div class="metric-row"><span class="metric-label">Оценочное время выполнения:</span> <span>{report.metrics.max_execution_time:.2f} ms</span></div>
            <div class="metric-row"><span class="metric-label">Оценочное количество строк:</span> <span>{report.metrics.total_rows:,}</span></div>
            <div class="metric-row"><span class="metric-label">Блоков с диска:</span> <span>{report.metrics.shared_read_blocks}</span></div>
            <div class="metric-row"><span class="metric-label">Параллельные воркеры:</span> <span>{report.metrics.parallel_workers}</span></div>
            <div class="metric-row"><span class="metric-label">Типы узлов плана:</span> <span>{', '.join(report.metrics.node_types)}</span></div>
        </div>

        <div class="section">
            <h2>Рекомендации по оптимизации</h2>
    """

    for i, rec in enumerate(report.recommendations, 1):
        priority_class = "low" if rec.priority.value == "low" else "medium" if rec.priority.value == "medium" else "high"
        html_content += f"""
            <div class="recommendation {priority_class}">
                <h3>Рекомендация #{i}: {rec.description}</h3>
                <p><strong>Тип:</strong> {rec.type}</p>
                <p><strong>Приоритет:</strong> {rec.priority.value}</p>
                <p><strong>Влияние:</strong> {rec.impact_score}/10</p>
                <p><strong>Ожидаемое улучшение:</strong> {rec.estimated_improvement}</p>
                <p><strong>Рекомендуемое действие:</strong> {rec.suggested_action}</p>
                <p><strong>Сервис T1 Cloud:</strong> {rec.t1_service.value if rec.t1_service else 'N/A'}</p>
            </div>
        """

    if report.warnings:
        html_content += """
            <div class="section">
                <h2>Предупреждения</h2>
        """
        for warning in report.warnings:
            html_content += f"<p class='warning'>• {warning}</p>"
        html_content += "</div>"

    # Итоговая оценка
    score_class = "score-high" if report.score >= 80 else "score-medium" if report.score >= 60 else "score-low"
    emoji = "✅" if report.score >= 80 else "⚠️" if report.score >= 60 else "❌"

    html_content += f"""
        <div class="section">
            <h2>Итоговая оценка</h2>
            <p class="{score_class}">{emoji} <strong>Оценка качества:</strong> {report.score}/100</p>
            <p><strong>Критичность:</strong> {'Да' if report.is_critical else 'Нет'}</p>
            <p><strong>Количество рекомендаций:</strong> {len(report.recommendations)}</p>
            <p><strong>Количество предупреждений:</strong> {len(report.warnings)}</p>
        </div>

        <div class="footer">
            Сгенерировано с помощью PGQueryGuard for T1 Cloud — {datetime.now().strftime('%Y-%m-%d')}
        </div>
    </body>
    </html>
    """

    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)

    HTML(string=html_content).write_pdf(output_path)
    print(f"PDF-отчет успешно сохранен: {output_path}")
