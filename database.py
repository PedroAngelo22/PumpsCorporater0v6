# database.py (Versão 5.4 - Correção final na estrutura do JSON)
import sqlite3
import streamlit as st
import httpx
import json

# --- Configurações do Banco de Dados ---
TURSO_DATABASE_URL = st.secrets["turso"]["DATABASE_URL"]
TURSO_AUTH_TOKEN = st.secrets["turso"]["DATABASE_TOKEN"]

# Função para executar comandos SQL no Turso
def execute_turso_query(query, params=None, fetch_mode='none'):
    headers = {
        "Authorization": f"Bearer {TURSO_AUTH_TOKEN}",
        "Content-Type": "application/json"
    }
    url = f"{TURSO_DATABASE_URL}/v1/execute"
    
    # --- LINHA CORRIGIDA FINAL ---
    # Garantimos que a chave 'params' sempre exista no objeto, mesmo que como uma lista vazia.
    statements = [{"stmt": query, "params": list(params) if params else []}]

    try:
        with httpx.Client() as client:
            response = client.post(url, headers=headers, json={"statements": statements})
            response.raise_for_status()
            result = response.json()
            if not result or not result.get('results'):
                return [] if fetch_mode == 'all' else None
            results_data = result['results'][0]
            if results_data.get('error'):
                raise Exception(f"Erro no Turso para a query '{query}': {results_data['error']}")
            if fetch_mode == 'none':
                return None
            columns = results_data.get('columns', [])
            rows = results_data.get('rows', [])
            if fetch_mode == 'one':
                if rows:
                    return {col: rows[0][i] for i, col in enumerate(columns)}
                return None
            elif fetch_mode == 'all':
                return [{col: row[i] for i, col in enumerate(columns)} for row in rows]
            return None
    except httpx.HTTPStatusError as e:
        st.error(f"Erro de HTTP ao conectar ao Turso: {e.response.status_code} - {e.response.text}")
        return None
    except httpx.RequestError as e:
        st.error(f"Erro de rede ao conectar ao Turso: {e}")
        return None
    except json.JSONDecodeError:
        st.error(f"Erro ao decodificar a resposta JSON do Turso. Resposta: {response.text}")
        return None
    except Exception as e:
        st.error(f"Erro inesperado ao executar query no Turso: {e}")
        return None

def setup_database():
    try:
        execute_turso_query("ALTER TABLE users ADD COLUMN email TEXT;")
    except Exception as e:
        if "duplicate column name" not in str(e):
            st.warning(f"Não foi possível adicionar a coluna 'email': {e}")
            
    queries = [
        """
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            name TEXT NOT NULL,
            email TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            project_name TEXT NOT NULL,
            FOREIGN KEY (username) REFERENCES users (username),
            UNIQUE (username, project_name)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS scenarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            project_name TEXT NOT NULL,
            scenario_name TEXT NOT NULL,
            scenario_data TEXT NOT NULL,
            FOREIGN KEY (username) REFERENCES users (username),
            FOREIGN KEY (project_name) REFERENCES projects (project_name),
            UNIQUE (username, project_name, scenario_name)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS user_fluids (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            fluid_name TEXT NOT NULL,
            density REAL NOT NULL,
            viscosity REAL NOT NULL,
            vapor_pressure REAL NOT NULL,
            FOREIGN KEY (username) REFERENCES users (username),
            UNIQUE (username, fluid_name)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS user_materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            material_name TEXT NOT NULL,
            roughness REAL NOT NULL,
            FOREIGN KEY (username) REFERENCES users (username),
            UNIQUE (username, material_name)
        );
        """
    ]
    for query in queries:
        execute_turso_query(query)


# --- Funções de Usuário (User Management) ---
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
    """Busca todos os usuários e formata para o streamlit-authenticator."""
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

# --- Funções de Cenários (Scenario Management) ---
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

# --- Funções de Fluidos Customizados ---
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

# --- Funções de Materiais Customizados ---
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
