# database.py (Versão 5.0 com conexão ao BD do Turso)

import sqlite3
import streamlit as st
import httpx
import json

# --- Configurações do Banco de Dados ---
# Usaremos uma abordagem para o Turso que emula o SQLite3, mas se conecta via HTTP.
# As credenciais são carregadas do Streamlit Secrets.
TURSO_DATABASE_URL = st.secrets["turso"]["DATABASE_URL"]
TURSO_AUTH_TOKEN = st.secrets["turso"]["DATABASE_TOKEN"]

# Nome do banco de dados local (para referência/criação inicial, se necessário)
DB_FILE = 'plataforma_hidraulica.db'

# Função para executar comandos SQL no Turso
def execute_turso_query(query, params=None, fetch_mode='none'):
    headers = {
        "Authorization": f"Bearer {TURSO_AUTH_TOKEN}",
        "Content-Type": "application/json"
    }
    url = f"{TURSO_DATABASE_URL}/v1/execute"

    # O Turso CLI envia um array de comandos, mesmo que seja um só.
    # Nós estamos simulando isso aqui.
    if params is None:
        statements = [{"q": query}]
    else:
        statements = [{"q": query, "params": list(params)}] # Turso espera params como lista

    try:
        with httpx.Client() as client:
            response = client.post(url, headers=headers, json={"statements": statements})
            response.raise_for_status() # Lança uma exceção para erros HTTP

            result = response.json()

            if not result or not result.get('results'):
                return [] if fetch_mode == 'all' else None # Nenhum resultado ou dados

            # A resposta do Turso é aninhada. Extraímos os dados.
            results_data = result['results'][0]

            if results_data.get('error'):
                raise Exception(f"Erro no Turso: {results_data['error']}")

            if fetch_mode == 'none':
                return None

            columns = results_data.get('columns', [])
            rows = results_data.get('rows', [])

            if fetch_mode == 'one':
                if rows:
                    # Retorna a primeira linha como um dicionário
                    return {col: rows[0][i] for i, col in enumerate(columns)}
                return None
            elif fetch_mode == 'all':
                # Retorna todas as linhas como uma lista de dicionários
                return [{col: row[i] for i, col in enumerate(columns)} for row in rows]

            return None # Default

    except httpx.HTTPStatusError as e:
        st.error(f"Erro de HTTP ao conectar ao Turso: {e.response.status_code} - {e.response.text}")
        return None
    except httpx.RequestError as e:
        st.error(f"Erro de rede ao conectar ao Turso: {e}")
        return None
    except json.JSONDecodeError:
        st.error(f"Erro ao decodificar a resposta JSON do Turso: {response.text}")
        return None
    except Exception as e:
        st.error(f"Erro inesperado ao executar query no Turso: {e}")
        return None

def setup_database():
    # As tabelas serão criadas se não existirem no Turso.
    # Não precisamos de um objeto de conexão 'conn' local aqui.
    queries = [
        """
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            name TEXT NOT NULL
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
    result = execute_turso_query("SELECT * FROM users WHERE username = ?", (username,), fetch_mode='one')
    return result

def add_user(username, password, name):
    try:
        execute_turso_query("INSERT INTO users (username, password, name) VALUES (?, ?, ?)", (username, password, name))
        return True
    except Exception as e:
        st.warning(f"Erro ao adicionar usuário: {e}. Pode ser que o usuário já exista.")
        return False

# --- Funções de Cenários (Scenario Management) ---
def save_scenario(username, project_name, scenario_name, scenario_data):
    # Primeiro, garante que o projeto exista
    execute_turso_query("INSERT OR IGNORE INTO projects (username, project_name) VALUES (?, ?)", (username, project_name))

    # Agora, salva ou atualiza o cenário
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
    # Opcional: Limpar projetos vazios
    # count_scenarios = execute_turso_query("SELECT COUNT(*) as count FROM scenarios WHERE username = ? AND project_name = ?", (username, project_name), fetch_mode='one')
    # if count_scenarios and count_scenarios['count'] == 0:
    #     execute_turso_query("DELETE FROM projects WHERE username = ? AND project_name = ?", (username, project_name))

# --- Funções de Fluidos Customizados ---
def add_user_fluid(username, fluid_name, density, viscosity, vapor_pressure):
    try:
        execute_turso_query(
            "INSERT INTO user_fluids (username, fluid_name, density, viscosity, vapor_pressure) VALUES (?, ?, ?, ?, ?)",
            (username, fluid_name, density, viscosity, vapor_pressure)
        )
        return True
    except Exception as e:
        # Em caso de UNIQUE constraint fail (fluido já existe)
        if "UNIQUE constraint failed" in str(e):
            return False
        raise # Re-lança outros erros

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
