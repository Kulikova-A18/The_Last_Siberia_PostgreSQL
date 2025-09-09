import psycopg2
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from .models import QueryMetric, Recommendation, AnalysisReport, Priority, T1CloudService
from .utils import extract_query_info, extract_filter_columns, find_plan_nodes, parse_psql_connection_string


logger = logging.getLogger("T1PgQueryAnalyzer")


class T1PgQueryAnalyzer:
    def __init__(self, dsn: str, t1_environment: Optional[str] = None, verbose: bool = False):
        self.raw_dsn = dsn
        self.dsn = None
        self.connection_params = None
        self.t1_environment = t1_environment
        self.verbose = verbose
        self.connection = None
        self.logger = logger

        dsn_stripped = dsn.strip()

        if "host=" in dsn_stripped and "port=" in dsn_stripped:
            try:
                self.connection_params = parse_psql_connection_string(dsn)
            except Exception as e:
                raise ValueError(f"Не удалось распарсить строку подключения как psql-формат: {e}")
        else:
            self.dsn = dsn

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                self.dsn,
                connect_timeout=10,
                application_name="pgqueryguard-t1-cloud"
            )
            self.connection.autocommit = True

            with self.connection.cursor() as cur:
                cur.execute("SELECT version(), current_database()")
                version_info, db_name = cur.fetchone()
                self.server_version = version_info
                self.db_name = db_name

        except psycopg2.Error as e:
            raise

    def get_explain_plan(self, query: str) -> Dict[str, Any]:
        with self.connection.cursor() as cur:
            try:
                explain_query = f"EXPLAIN (FORMAT JSON, VERBOSE, SETTINGS, BUFFERS) {query}"
                cur.execute(explain_query)
                result = cur.fetchone()

                if result and result[0]:
                    plan_data = json.loads(result[0])
                    return plan_data
                else:
                    raise Exception("Пустой результат EXPLAIN")

            except psycopg2.Error:
                try:
                    explain_query = f"EXPLAIN (FORMAT JSON) {query}"
                    cur.execute(explain_query)
                    result = cur.fetchone()
                    if result and result[0]:
                        plan_data = json.loads(result[0])
                        return plan_data
                except psycopg2.Error as e:
                    raise Exception(f"Не удалось получить план выполнения: {e}")

    def extract_metrics(self, plan: Dict[str, Any]) -> QueryMetric:
        total_plan = plan[0]['Plan']

        metrics = QueryMetric(
            total_cost=total_plan['Total Cost'],
            planning_time=total_plan.get('Planning Time'),
            max_execution_time=self.estimate_execution_time(total_plan['Total Cost']),
            shared_hit_blocks=total_plan.get('Shared Hit Blocks', 0),
            shared_read_blocks=total_plan.get('Shared Read Blocks', 0),
            plan_width=total_plan['Plan Width'],
            total_rows=total_plan['Plan Rows'],
            node_types=self.extract_node_types(total_plan),
            startup_cost=total_plan.get('Startup Cost', 0),
            total_workers=total_plan.get('Workers', 0),
            parallel_workers=total_plan.get('Workers Launched', 0)
        )

        return metrics

    def extract_node_types(self, plan_node: Dict[str, Any]) -> List[str]:
        node_types = []

        def _extract_nodes(node):
            if 'Node Type' in node:
                node_types.append(node['Node Type'])
            if 'Plans' in node:
                for child in node['Plans']:
                    _extract_nodes(child)

        _extract_nodes(plan_node)
        return list(set(node_types))

    def analyze_plan_structure(self, plan: Dict[str, Any]) -> None:
        total_plan = plan[0]['Plan']

        def build_tree(node, parent_tree=None):
            if 'Plans' in node:
                for child in node['Plans']:
                    build_tree(child, parent_tree)

        build_tree(total_plan)

    def estimate_execution_time(self, total_cost: float) -> float:
        return total_cost * 0.01

    def extract_indexes_used(self, plan: Dict[str, Any]) -> List[str]:
        indexes = []

        def _find_indexes(node):
            if node.get('Node Type') == 'Index Scan':
                index_name = node.get('Index Name', 'unknown')
                relation_name = node.get('Relation Name', 'unknown')
                indexes.append(f"{relation_name}({index_name})")

            if 'Plans' in node:
                for child in node['Plans']:
                    _find_indexes(child)

        _find_indexes(plan[0]['Plan'])
        return indexes

    def generate_warnings(self, plan: Dict[str, Any], query: str) -> List[str]:
        warnings = []
        total_plan = plan[0]['Plan']

        joins = find_plan_nodes(total_plan, 'Nested Loop')
        if len(joins) > 2:
            warnings.append("Возможное Cartesian product в JOIN операциях")

        if ('SELECT' in query.upper() and 'LIMIT' not in query.upper() and
                total_plan['Plan Rows'] > 10000):
            warnings.append("Большая выборка без LIMIT - риск высокой нагрузки")

        return warnings

    def generate_t1_recommendations(self, plan: Dict[str, Any], query: str) -> List[Recommendation]:
        recommendations = []
        total_plan = plan[0]['Plan']

        seq_scans = find_plan_nodes(total_plan, 'Seq Scan')
        for scan in seq_scans:
            if scan['Plan Rows'] > 10000 and 'Filter' in scan:
                table_name = scan.get('Relation Name', 'unknown')
                rec = Recommendation(
                    type="missing_index",
                    description=f"Полное сканирование большой таблицы {table_name} ({scan['Plan Rows']:,} строк)",
                    priority=Priority.HIGH,
                    estimated_improvement="Ускорение на 80-95%",
                    suggested_action=f"Создать индекс на {table_name}({extract_filter_columns(scan)})",
                    affected_components=[table_name],
                    t1_service=T1CloudService.POSTGRESQL,
                    impact_score=9
                )
                recommendations.append(rec)

        sort_nodes = find_plan_nodes(total_plan, 'Sort')
        for sort_node in sort_nodes:
            if sort_node.get('Sort Method') == 'external':
                rec = Recommendation(
                    type="disk_sort",
                    description="Сортировка выполняется на диске (медленно)",
                    priority=Priority.MEDIUM,
                    estimated_improvement="Ускорение на 50-70%",
                    suggested_action="Увеличить work_mem или оптимизировать ORDER BY",
                    affected_components=["PostgreSQL Configuration"],
                    t1_service=T1CloudService.POSTGRESQL,
                    impact_score=6
                )
                recommendations.append(rec)

        nested_loops = find_plan_nodes(total_plan, 'Nested Loop')
        for loop in nested_loops:
            if loop['Plan Rows'] > 1000:
                rec = Recommendation(
                    type="inefficient_join",
                    description="Неэффективное вложенное соединение для большого набора данных",
                    priority=Priority.MEDIUM,
                    estimated_improvement="Ускорение на 30-60%",
                    suggested_action="Рассмотреть Hash Join или Merge Join, добавить индексы",
                    affected_components=["Join Operations"],
                    t1_service=T1CloudService.POSTGRESQL,
                    impact_score=7
                )
                recommendations.append(rec)

        return recommendations

    def calculate_score(self, metrics: QueryMetric, recommendations: List[Recommendation]) -> int:
        base_score = 100

        if metrics.total_cost > 10000:
            base_score -= 30
        elif metrics.total_cost > 5000:
            base_score -= 20
        elif metrics.total_cost > 1000:
            base_score -= 10

        if metrics.shared_read_blocks > 1000:
            base_score -= 25
        elif metrics.shared_read_blocks > 500:
            base_score -= 15

        for rec in recommendations:
            if rec.priority == Priority.HIGH:
                base_score -= 30
            elif rec.priority == Priority.MEDIUM:
                base_score -= 15

        return max(0, min(100, base_score))

    def analyze_query(self, query: str) -> AnalysisReport:
        query_info = extract_query_info(query)

        plan = self.get_explain_plan(query)

        if self.verbose:
            self.analyze_plan_structure(plan)

        metrics = self.extract_metrics(plan)
        recommendations = self.generate_t1_recommendations(plan, query)

        report = AnalysisReport(
            query=query,
            metrics=metrics,
            recommendations=recommendations,
            is_critical=any(r.priority == Priority.HIGH for r in recommendations),
            score=self.calculate_score(metrics, recommendations),
            t1_environment=self.t1_environment,
            query_type=query_info['type'],
            tables_affected=query_info['tables'],
            indexes_used=self.extract_indexes_used(plan),
            warnings=self.generate_warnings(plan, query)
        )

        return report