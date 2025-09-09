from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime

class T1CloudService(str, Enum):
    POSTGRESQL = "Managed Service for PostgreSQL"
    KUBERNETES = "Managed Service for Kubernetes"
    CLICKHOUSE = "Managed Service for ClickHouse"
    KAFKA = "Managed Service for Kafka"
    OPENSEARCH = "Managed Service for OpenSearch"
    REDIS = "Managed Service for InmemoryDB"
    DOCUMENTDB = "Managed Service for DocumentDB"
    RABBITMQ = "Managed Service for RabbitMQ"

class Priority(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"

class QueryMetric(BaseModel):
    total_cost: float = Field(..., description="Общая стоимость запроса")
    planning_time: Optional[float] = Field(None, description="Время планирования (мс)")
    max_execution_time: float = Field(..., description="Оценка времени выполнения (мс)")
    shared_hit_blocks: int = Field(0, description="Блоки из кэша")
    shared_read_blocks: int = Field(0, description="Блокы с диска")
    plan_width: int = Field(..., description="Ширина плана (байт)")
    total_rows: int = Field(..., description="Оценочное количество строк")
    node_types: List[str] = Field(..., description="Типы узлов в плане")
    startup_cost: float = Field(..., description="Стоимость запуска")
    total_workers: int = Field(0, description="Общее количество воркеров")
    parallel_workers: int = Field(0, description="Количество параллельных воркеров")

class Recommendation(BaseModel):
    type: str = Field(..., description="Тип рекомендации")
    description: str = Field(..., description="Описание проблемы")
    priority: Priority = Field(..., description="Приоритет")
    estimated_improvement: str = Field(..., description="Оценка улучшения")
    suggested_action: str = Field(..., description="Предлагаемое действие")
    affected_components: List[str] = Field(..., description="Затронутые компоненты")
    t1_service: Optional[T1CloudService] = Field(None, description="Связанный сервис T1 Cloud")
    impact_score: int = Field(..., description="Влияние на производительность (1-10)")

class AnalysisReport(BaseModel):
    query: str = Field(..., description="Анализируемый запрос")
    metrics: QueryMetric = Field(..., description="Метрики производительности")
    recommendations: List[Recommendation] = Field(..., description="Рекомендации")
    is_critical: bool = Field(..., description="Критическая проблема")
    score: int = Field(..., description="Оценка качества (0-100)")
    analyzed_at: datetime = Field(default_factory=datetime.now, description="Время анализа")
    t1_environment: Optional[str] = Field(None, description="Окружение T1 Cloud")
    query_type: str = Field(..., description="Тип запроса (SELECT, INSERT, etc.)")
    tables_affected: List[str] = Field(..., description="Затронутые таблицы")
    indexes_used: List[str] = Field(..., description="Используемые индексы")
    warnings: List[str] = Field(..., description="Предупреждения")
