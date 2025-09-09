import random
from datetime import datetime
from .models import QueryMetric, Recommendation, AnalysisReport, Priority, T1CloudService
from .pdf_report import generate_pdf_report

TABLES = ["orders", "customers", "products", "invoices", "payments", "users", "sessions", "logs"]
COLUMNS = ["id", "name", "email", "status", "created_at", "amount", "category", "price", "quantity"]
JOIN_CONDITIONS = [
    "o.customer_id = c.id",
    "p.category_id = c.id",
    "i.order_id = o.id",
    "u.role_id = r.id",
    "l.user_id = u.id"
]
FILTERS = [
    "status = 'active'",
    "created_at > '2024-01-01'",
    "amount > 1000",
    "category = 'electronics'",
    "quantity < 10"
]
SORTS = ["created_at DESC", "amount DESC", "name ASC", "id DESC"]
RECOMMENDATION_TYPES = [
    ("missing_index", "Полное сканирование большой таблицы {table} ({rows} строк)", "Создать индекс на {table}({column})", ["{table}"], 9),
    ("disk_sort", "Сортировка выполняется на диске (медленно)", "Увеличить work_mem или оптимизировать ORDER BY", ["PostgreSQL Configuration"], 6),
    ("inefficient_join", "Неэффективное вложенное соединение для большого набора данных", "Рассмотреть Hash Join или Merge Join, добавить индексы", ["Join Operations"], 7),
    ("missing_statistics", "Отсутствует статистика для таблицы {table}", "Выполнить ANALYZE {table}", ["{table}"], 5),
    ("unindexed_foreign_key", "Внешний ключ {column} не имеет индекса", "Создать индекс на {table}({column})", ["{table}"], 8),
]
WARNINGS = [
    "Большая выборка без LIMIT - риск высокой нагрузки",
    "Возможное Cartesian product в JOIN операциях",
    "Фильтрация по неиндексированному полю",
    "Использование SELECT * вместо явного перечисления полей",
    "Отсутствует ORDER BY при использовании LIMIT"
]

def generate_random_query():
    query_type = random.choice(["SELECT", "UPDATE", "DELETE", "INSERT"])
    tables = random.sample(TABLES, k=random.randint(1, 3))
    main_table = tables[0]

    if query_type == "SELECT":
        select_fields = "*" if random.random() > 0.5 else ", ".join(random.sample(COLUMNS, k=random.randint(1, 3)))
        where_clause = ""
        if random.random() > 0.3:
            where_clause = " WHERE " + " AND ".join(random.sample(FILTERS, k=random.randint(1, 2)))
        join_clause = ""
        if len(tables) > 1:
            join_table = tables[1]
            join_condition = random.choice(JOIN_CONDITIONS).replace("o.", f"{main_table[0]}.").replace("c.", f"{join_table[0]}.")
            join_clause = f" JOIN {join_table} {join_table[0]} ON {join_condition}"
        order_clause = ""
        if random.random() > 0.5:
            order_clause = " ORDER BY " + random.choice(SORTS)
        limit_clause = ""
        if random.random() > 0.7:
            limit_clause = " LIMIT " + str(random.choice([10, 50, 100, 1000]))
        query = f"SELECT {select_fields} FROM {main_table} {main_table[0]}{join_clause}{where_clause}{order_clause}{limit_clause}"
    elif query_type == "UPDATE":
        set_clause = f"SET {random.choice(COLUMNS)} = {random.choice(['NULL', '0', "'updated'"])}"
        where_clause = " WHERE " + random.choice(FILTERS) if random.random() > 0.3 else ""
        query = f"UPDATE {main_table} {set_clause}{where_clause}"
    elif query_type == "DELETE":
        where_clause = " WHERE " + random.choice(FILTERS) if random.random() > 0.3 else ""
        query = f"DELETE FROM {main_table}{where_clause}"
    else:  # INSERT
        columns = random.sample(COLUMNS, k=random.randint(2, 4))
        values = ", ".join([f"'value{i}'" if random.random() > 0.5 else str(random.randint(1, 100)) for i in range(len(columns))])
        query = f"INSERT INTO {main_table} ({', '.join(columns)}) VALUES ({values})"

    return query.strip(), query_type, tables

def generate_random_metrics():
    total_cost = random.uniform(100, 50000)
    return QueryMetric(
        total_cost=total_cost,
        startup_cost=random.uniform(0, total_cost * 0.3),
        max_execution_time=total_cost * 0.01,
        shared_read_blocks=random.randint(0, 5000),
        plan_width=random.randint(32, 512),
        total_rows=int(random.uniform(100, 2000000)),
        node_types=random.sample(["Seq Scan", "Index Scan", "Nested Loop", "Hash Join", "Merge Join", "Sort", "Aggregate"], k=random.randint(2, 5)),
        total_workers=random.randint(0, 4),
        parallel_workers=random.randint(0, 3)
    )

def generate_random_recommendations(tables, has_join):
    recommendations = []
    num_recs = random.randint(0, 4)
    for _ in range(num_recs):
        rec_type, desc_template, action_template, components, impact = random.choice(RECOMMENDATION_TYPES)
        table = random.choice(tables) if tables else "unknown"
        column = random.choice(COLUMNS)
        desc = desc_template.format(table=table, rows=random.randint(10000, 2000000))
        action = action_template.format(table=table, column=column)
        comps = [c.format(table=table) for c in components]
        priority = random.choice([Priority.HIGH, Priority.MEDIUM, Priority.LOW])
        if priority == Priority.LOW:
            impact = random.randint(1, 4)
        elif priority == Priority.MEDIUM:
            impact = random.randint(5, 7)
        else:
            impact = random.randint(8, 10)

        rec = Recommendation(
            type=rec_type,
            description=desc,
            priority=priority,
            estimated_improvement=f"Ускорение на {random.randint(30,95)}%",
            suggested_action=action,
            affected_components=comps,
            t1_service=T1CloudService.POSTGRESQL,
            impact_score=impact
        )
        recommendations.append(rec)

    if has_join and random.random() > 0.5 and len(recommendations) < 4:
        rec = Recommendation(
            type="inefficient_join",
            description="Неоптимальная стратегия соединения таблиц",
            priority=Priority.MEDIUM,
            estimated_improvement="Ускорение на 40-60%",
            suggested_action="Рассмотреть использование Hash Join вместо Nested Loop",
            affected_components=["Join Strategy"],
            t1_service=T1CloudService.POSTGRESQL,
            impact_score=7
        )
        recommendations.append(rec)

    return recommendations


def generate_random_warnings(query_type, has_limit, has_order_by):
    warnings = []
    if random.random() > 0.5:
        warnings.append(random.choice(WARNINGS))

    if query_type == "SELECT" and "LIMIT" not in query_type and random.random() > 0.7:
        warnings.append("Большая выборка без LIMIT - риск высокой нагрузки")

    if "JOIN" in query_type and random.random() > 0.6:
        warnings.append("Возможное Cartesian product в JOIN операциях")

    if "*" in query_type and random.random() > 0.6:
        warnings.append("Использование SELECT * вместо явного перечисления полей")

    return warnings


def demo_analysis():
    query, query_type, tables = generate_random_query()
    has_join = "JOIN" in query
    has_limit = "LIMIT" in query
    has_order_by = "ORDER BY" in query

    metrics = generate_random_metrics()
    recommendations = generate_random_recommendations(tables, has_join)
    warnings = generate_random_warnings(query_type, has_limit, has_order_by)

    is_critical = any(r.priority == Priority.HIGH for r in recommendations)
    score = max(0, min(100, 100 - sum(15 if r.priority == Priority.HIGH else 7 if r.priority == Priority.MEDIUM else 3 for r in recommendations) - (10 if metrics.total_cost > 10000 else 5 if metrics.total_cost > 5000 else 0)))

    mock_report = AnalysisReport(
        query=query,
        metrics=metrics,
        recommendations=recommendations,
        is_critical=is_critical,
        score=score,
        t1_environment="demo",
        query_type=query_type,
        tables_affected=tables,
        indexes_used=[f"{random.choice(tables)}_idx" for _ in range(random.randint(0, 2))] if tables else [],
        warnings=warnings
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    pdf_filename = f"output/demo_report_{timestamp}.pdf"
    generate_pdf_report(mock_report, pdf_filename)