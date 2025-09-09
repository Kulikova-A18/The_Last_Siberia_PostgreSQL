import re
from typing import Dict, Any, List

def parse_psql_connection_string(psql_str: str) -> Dict[str, str]:
    psql_str = psql_str.strip()

    if psql_str.startswith('psql'):
        psql_str = psql_str[4:].strip()

    if (psql_str.startswith('"') and psql_str.endswith('"')) or \
       (psql_str.startswith("'") and psql_str.endswith("'")):
        psql_str = psql_str[1:-1].strip()

    params = {}
    pattern = r'(\w+)=([^\s]+)'
    matches = re.findall(pattern, psql_str)

    for key, value in matches:
        params[key] = value

    required = ['host', 'port', 'user', 'dbname']
    for r in required:
        if r not in params:
            raise ValueError(f"Обязательный параметр '{r}' отсутствует в строке подключения: {psql_str}")
    return params

def extract_query_info(query: str) -> Dict[str, Any]:
    query_upper = query.upper()

    info = {
        'type': 'UNKNOWN',
        'tables': [],
        'operations': []
    }

    if query_upper.startswith('SELECT'):
        info['type'] = 'SELECT'
    elif query_upper.startswith('INSERT'):
        info['type'] = 'INSERT'
    elif query_upper.startswith('UPDATE'):
        info['type'] = 'UPDATE'
    elif query_upper.startswith('DELETE'):
        info['type'] = 'DELETE'

    table_matches = re.findall(r'\b(FROM|JOIN|INTO|UPDATE)\s+(\w+)', query_upper)
    info['tables'] = list(set([match[1] for match in table_matches]))

    if 'WHERE' in query_upper:
        info['operations'].append('FILTER')
    if 'ORDER BY' in query_upper:
        info['operations'].append('SORT')
    if 'GROUP BY' in query_upper:
        info['operations'].append('AGGREGATE')
    if 'JOIN' in query_upper:
        info['operations'].append('JOIN')
    return info

def extract_filter_columns(scan_node: Dict[str, Any]) -> str:
    filter_str = str(scan_node.get('Filter', ''))
    column_matches = re.findall(r'\((\w+)\)', filter_str)
    if column_matches:
        return ', '.join(set(column_matches))
    return "column_name"


def find_plan_nodes(plan_node: dict[str, Any], node_type: str) -> List[dict[str, Any]]:
    nodes = []
    def _find_nodes(node):
        if node.get('Node Type') == node_type:
            nodes.append(node)
        if 'Plans' in node:
            for child in node['Plans']:
                _find_nodes(child)
    _find_nodes(plan_node)
    return nodes
