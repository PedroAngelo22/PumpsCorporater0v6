# database.py (Versão 7.2 - Desempacotamento da resposta da API do Turso)
import sqlite3
import streamlit as st
import httpx
import json

# --- Configurações do Banco de Dados ---
TURSO_DATABASE_URL = st.secrets["turso"]["DATABASE_URL"]
TURSO_AUTH_TOKEN = st.secrets["turso"]["DATABASE_TOKEN"]

# --- FUNÇÕES AUXILIARES ---
def _format_turso_args(params):
    """Formata os parâmetros de ida para o formato exigido pela API v2."""
    if not params:
        return []
    
    formatted_args = []
    for p in params:
        if isinstance(p, str):
            formatted_args.append({"type": "text", "value": p})
        elif isinstance(p, int):
            formatted_args.append({"type": "integer", "value": str(p)})
        elif isinstance(p, float):
            formatted_args.append({"type": "float", "value": p})
        elif p is None:
            formatted_args.append({"type": "null"})
        else:
            formatted_args.append({"type": "text", "value": str(p)})
    return formatted_args

def _unwrap_turso_response_values(rows, columns):
    """'Desempacota' os objetos de valor retornados pela API v2."""
    if not rows:
        return []
    
    unwrapped_rows = []
    for row in rows:
        unwrapped_row = []
        for val in row:
            if isinstance(val, dict) and 'value' in val:
                unwrapped_row.append(val['value'])
            else:
                unwrapped_row.append(val)
        unwrapped_rows.append(unwrapped_row)
    
    # Converte as linhas desempacotadas em dicionários
    dict_rows = []
    for row in unwrapped_rows:
        dict_rows.append({columns[i]: row[i] for i in range(len(columns))})
        
    return dict_rows

# --- Função de query com a lógica final ---
def execute_turso_query(query, params=None, fetch_mode='none'):
    headers = {
        "Authorization": f"Bearer {TURSO_AUTH_TOKEN}",
        "Content-Type": "application/json"
    }
    url = f"{TURSO_DATABASE_URL}/v2/pipeline"
    
    formatted_args = _format_turso_args(params)
    stmt_obj = {"sql": query, "args": formatted_args}
    request_obj = {"type": "execute", "stmt": stmt_obj}
    payload = {"requests": [request_obj]}

    try:
        with httpx.Client() as client:
            response = client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            json_response = response.json()

            if not json_response or 'results' not in json_response or not json_response['results']:
                return [] if fetch_mode == 'all' else None

            first_result = json_response['results'][0]

            if first_result.get('type') == 'error':
                error_info = first_result.get('error', {})
                raise Exception(f"{error_info.get('message', 'Erro desconhecido')}")

            if fetch_mode == 'none':
                return None
            
            result_data = first_result.get('response', {}).get('result', {})
            columns = [col.get('name') for col in result_data.get('cols', []) if col.get('name') is not None]
            rows = result_data.get('rows', [])

            if not columns or not rows:
                return [] if fetch_mode == 'all' else None

            # Usamos a nova função para processar os resultados
            processed_rows = _unwrap_turso_response_values(rows, columns)

            if fetch_mode == 'one':
                return processed_rows[0] if processed_rows else None
            elif fetch_mode == 'all':
                return processed_rows
            return None
    except Exception as e:
        raise e

def setup_database():
    try:
        execute_turso_query("ALTER TABLE users ADD COLUMN email TEXT;")
    except Exception as e:
        if "duplicate column name" not in str(e):
            st.warning(f"Não foi possível adicionar a coluna 'email': {e}")
            
    queries = [
        "CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password TEXT NOT NULL, name TEXT NOT NULL, email TEXT);",
        "CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY, username TEXT NOT NULL, project_name TEXT NOT NULL, FOREIGN KEY (username) REFERENCES users (username), UNIQUE (username, project_name));",
        "CREATE TABLE IF NOT EXISTS scenarios (id INTEGER PRIMARY KEY, username TEXT NOT NULL, project_name TEXT NOT NULL, scenario_name TEXT NOT NULL, scenario_data TEXT NOT NULL, FOREIGN KEY (username) REFERENCES users (username), UNIQUE (username, project_name, scenario_name));",
        "CREATE TABLE IF NOT EXISTS user_fluids (id INTEGER PRIMARY KEY, username TEXT NOT NULL, fluid_name TEXT NOT NULL, density REAL NOT NULL, viscosity REAL NOT NULL, vapor_pressure REAL NOT NULL, FOREIGN KEY (username) REFERENCES users (username), UNIQUE (username, fluid_name));",
        "CREATE TABLE IF NOT EXISTS user_materials (id INTEGER PRIMARY KEY, username TEXT NOT NULL, material_name TEXT NOT NULL, roughness REAL NOT NULL, FOREIGN KEY (username) REFERENCES users (username), UNIQUE (username, material_name));"
    ]
    for query in queries:
        execute_turso_query(query)

def get_user(username):
    result = execute_turso_query("SELECT username, password, name, email FROM users WHERE username = ?", (username,), fetch_mode='one')
    return result

def add_user(username, password_hashed, name, email):
    try:
        execute_turso_query("INSERT INTO users (username, password, name, email) VALUES (?, ?, ?, ?)", (username, password_hashed, name, email))
        return True
    except Exception as e:
        if "invalid type: string" in str(e) and "expected internally tagged enum" in str(e):
            return True
        if "UNIQUE constraint failed" in str(e):
            return False
        st.error(f"Erro inesperado ao adicionar usuário: {e}")
        return False
        
def save_scenario(username, project_name, scenario_name, scenario_data):
    execute_turso_query("INSERT OR IGNORE INTO projects (username, project_name) VALUES (?, ?)", (username, project_name))
    data_json = json.dumps(scenario_data)
    execute_turso_query(
        "INSERT OR REPLACE INTO scenarios (username, project_name, scenario_name, scenario_data) VALUES (?, ?, ?, ?)",
        (username, project_name, scenario_name, data_json)
    )

def load_scenario(username, project_name, scenario_name):
    result = execute_turso_query(
        "SELECT scenario_data FROM scenarios WHERE username = ? AND project_name = ? AND scenario_name = ?",
        (username, project_name, scenario_name),
        fetch_mode='one'
    )
    if result and 'scenario_data' in result:
        return json.loads(result['scenario_data'])
    return None

def get_user_projects(username):
    results = execute_turso_query("SELECT project_name FROM projects WHERE username = ?", (username,), fetch_mode='all')
    return [row['project_name'] for row in results] if results else []

def get_scenarios_for_project(username, project_name):
    results = execute_turso_query(
        "SELECT scenario_name FROM scenarios WHERE username = ? AND project_name = ?",
        (username, project_name),
        fetch_mode='all'
    )
    return [row['scenario_name'] for row in results] if results else []

def delete_scenario(username, project_name, scenario_name):
    execute_turso_query(
        "DELETE FROM scenarios WHERE username = ? AND project_name = ? AND scenario_name = ?",
        (username, project_name, scenario_name)
    )

def add_user_fluid(username, fluid_name, density, viscosity, vapor_pressure):
    try:
        execute_turso_query(
            "INSERT INTO user_fluids (username, fluid_name, density, viscosity, vapor_pressure) VALUES (?, ?, ?, ?, ?)",
            (username, fluid_name, density, viscosity, vapor_pressure)
        )
        return True
    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            return False
        raise

def get_user_fluids(username):
    results = execute_turso_query("SELECT fluid_name, density, viscosity, vapor_pressure FROM user_fluids WHERE username = ?", (username,), fetch_mode='all')
    if not results: return {}
    return {row['fluid_name']: {'rho': row['density'], 'nu': row['viscosity'], 'pv_kpa': row['vapor_pressure']} for row in results}

def delete_user_fluid(username, fluid_name):
    execute_turso_query("DELETE FROM user_fluids WHERE username = ? AND fluid_name = ?", (username, fluid_name))

def add_user_material(username, material_name, roughness):
    try:
        execute_turso_query(
            "INSERT INTO user_materials (username, material_name, roughness) VALUES (?, ?, ?)",
            (username, material_name, roughness)
        )
        return True
    except Exception as e:
        if "UNIQUE constraint failed" in str(e):
            return False
        raise

def get_user_materials(username):
    results = execute_turso_query("SELECT material_name, roughness FROM user_materials WHERE username = ?", (username,), fetch_mode='all')
    if not results: return {}
    return {row['material_name']: row['roughness'] for row in results}

def delete_user_material(username, material_name):
    execute_turso_query("DELETE FROM user_materials WHERE username = ? AND material_name = ?", (username, material_name))
