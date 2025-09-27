# database.py (Versão 7.0 - Correção definitiva da formatação de parâmetros)
import sqlite3
import streamlit as st
import httpx
import json

# --- Configurações do Banco de Dados ---
TURSO_DATABASE_URL = st.secrets["turso"]["DATABASE_URL"]
TURSO_AUTH_TOKEN = st.secrets["turso"]["DATABASE_TOKEN"]

# --- NOVA FUNÇÃO AUXILIAR ---
def _format_turso_args(params):
    """Formata os parâmetros para o formato exigido pela API v2 do Turso."""
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
        else: # Um fallback seguro para outros tipos de dados
            formatted_args.append({"type": "text", "value": str(p)})
    return formatted_args

# --- Função de query atualizada para usar a formatação ---
def execute_turso_query(query, params=None, fetch_mode='none'):
    headers = {
        "Authorization": f"Bearer {TURSO_AUTH_TOKEN}",
        "Content-Type": "application/json"
    }
    url = f"{TURSO_DATABASE_URL}/v2/pipeline"
    
    # Usamos a nova função para formatar os parâmetros
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

            if not columns: return [] if fetch_mode == 'all' else None
            if not rows and fetch_mode != 'none': return [] if fetch_mode == 'all' else None

            if fetch_mode == 'one':
                if rows: return {columns[i]: row[i] for i in range(len(columns))}
                return None
            elif fetch_mode == 'all':
                return [{columns[i]: row[i] for i in range(len(columns))} for row in rows]
            return None
    except Exception as e:
        raise e

def setup_database():
    # ... (o resto do arquivo não precisa de mudanças, pois já usa as funções acima)
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
        if "UNIQUE constraint failed" in str(e):
            return False
        st.error(f"Erro inesperado ao adicionar usuário: {e}")
        return False
        
def get_all_users_for_auth():
    users = execute_turso_query("SELECT username, name, password, email FROM users", fetch_mode='all')
    credentials = {"usernames": {}}
    if users:
        for user in users:
            credentials["usernames"][user['username']] = {
                "name": user['name'],
                "password": user['password'],
                "email": user.get('email', '')
            }
    return credentials

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
    if results:
        return {row['fluid_name']: {'rho': row['density'], 'nu': row['viscosity'], 'pv_kpa': row['vapor_pressure']} for row in results}
    return {}

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
    if results:
        return {row['material_name']: row['roughness'] for row in results}
    return {}

def delete_user_material(username, material_name):
    execute_turso_query("DELETE FROM user_materials WHERE username = ? AND material_name = ?", (username, material_name))
