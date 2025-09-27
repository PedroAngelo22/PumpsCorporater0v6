# database.py (Versão 4.0 com Pressão de Vapor)

import sqlite3
import json
from datetime import datetime

DB_NAME = 'plataforma_hidraulica.db'

def setup_database():
    """Cria/atualiza todas as tabelas necessárias no banco de dados."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    # Tabela de Cenários (sem alteração)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scenarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            project_name TEXT NOT NULL,
            scenario_name TEXT NOT NULL,
            scenario_data TEXT NOT NULL,
            last_modified TIMESTAMP NOT NULL,
            UNIQUE(username, project_name, scenario_name)
        )
    ''')
    
    # Tabela de Fluidos com a nova coluna para pressão de vapor
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_fluids (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            fluid_name TEXT NOT NULL,
            density REAL NOT NULL,
            kinematic_viscosity REAL NOT NULL,
            vapor_pressure_kpa REAL, -- <<< COLUNA ADICIONADA
            UNIQUE(username, fluid_name)
        )
    ''')

    # Tabela de Materiais (sem alteração)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            material_name TEXT NOT NULL,
            roughness REAL NOT NULL,
            UNIQUE(username, material_name)
        )
    ''')

    conn.commit()
    conn.close()

# --- Funções de Cenários (sem alteração) ---
def save_scenario(username, project_name, scenario_name, scenario_data):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    scenario_data_json = json.dumps(scenario_data)
    timestamp = datetime.now()
    cursor.execute('''
        INSERT OR REPLACE INTO scenarios (id, username, project_name, scenario_name, scenario_data, last_modified)
        VALUES ((SELECT id FROM scenarios WHERE username = ? AND project_name = ? AND scenario_name = ?), ?, ?, ?, ?, ?)
    ''', (username, project_name, scenario_name, username, project_name, scenario_name, scenario_data_json, timestamp))
    conn.commit()
    conn.close()
    return True

def load_scenario(username, project_name, scenario_name):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT scenario_data FROM scenarios WHERE username = ? AND project_name = ? AND scenario_name = ?", (username, project_name, scenario_name))
    result = cursor.fetchone()
    conn.close()
    if result:
        return json.loads(result[0])
    return None

def get_user_projects(username):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT project_name FROM scenarios WHERE username = ? ORDER BY project_name ASC", (username,))
    projects = [row[0] for row in cursor.fetchall()]
    conn.close()
    return projects

def get_scenarios_for_project(username, project_name):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT scenario_name FROM scenarios WHERE username = ? AND project_name = ? ORDER BY last_modified DESC", (username, project_name))
    scenarios = [row[0] for row in cursor.fetchall()]
    conn.close()
    return scenarios

def delete_scenario(username, project_name, scenario_name):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM scenarios WHERE username = ? AND project_name = ? AND scenario_name = ?", (username, project_name, scenario_name))
    conn.commit()
    conn.close()
    return True

# --- FUNÇÕES DA BIBLIOTECA ATUALIZADAS ---

# --- Fluidos ---
# 1. Função ATUALIZADA para aceitar o novo parâmetro
def add_user_fluid(username, fluid_name, density, kinematic_viscosity, vapor_pressure_kpa):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        # 2. Comando INSERT ATUALIZADO para incluir a nova coluna
        cursor.execute(
            "INSERT INTO user_fluids (username, fluid_name, density, kinematic_viscosity, vapor_pressure_kpa) VALUES (?, ?, ?, ?, ?)",
            (username, fluid_name, density, kinematic_viscosity, vapor_pressure_kpa)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()
    return True

# 3. Função ATUALIZADA para buscar o novo dado
def get_user_fluids(username):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    # 4. Comando SELECT ATUALIZADO para buscar a nova coluna
    cursor.execute("SELECT fluid_name, density, kinematic_viscosity, vapor_pressure_kpa FROM user_fluids WHERE username = ?", (username,))
    # 5. Dicionário ATUALIZADO para incluir o novo dado 'pv_kpa'
    fluids = {row[0]: {'rho': row[1], 'nu': row[2], 'pv_kpa': row[3]} for row in cursor.fetchall()}
    conn.close()
    return fluids

def delete_user_fluid(username, fluid_name):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM user_fluids WHERE username = ? AND fluid_name = ?", (username, fluid_name))
    conn.commit()
    conn.close()
    return True

# --- Materiais (sem alteração) ---
def add_user_material(username, material_name, roughness):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO user_materials (username, material_name, roughness) VALUES (?, ?, ?)",
                       (username, material_name, roughness))
        conn.commit()
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()
    return True

def get_user_materials(username):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("SELECT material_name, roughness FROM user_materials WHERE username = ?", (username,))
    materials = {row[0]: row[1] for row in cursor.fetchall()}
    conn.close()
    return materials

def delete_user_material(username, material_name):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM user_materials WHERE username = ? AND material_name = ?", (username, material_name))
    conn.commit()
    conn.close()
    return True
