import os
import requests
import pandas as pd
import psycopg2

# URL del archivo de datos
data_url = 'https://download.bls.gov/pub/time.series/ce/ce.data.0.AllCESSeries'
# Series necesarias
series = {'women_gov': 'CES9000000010', 'prod_nonsuper_employ': 'CES0500000006', 'all_employees': 'CES0500000001'}

def download_and_process(url, series_id):
    filtered_lines = []
    print(f"Descargando {url}...")
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, stream=True, headers=headers)
    
    if response.status_code != 200:
        print(f"Error al descargar {url}")
        return
    
    print(f"Procesando {url}...")
    first_line = True
    for line in response.iter_lines(decode_unicode=True):
        if first_line:
            header = [col.strip() for col in line.split('\t')]
            first_line = False
        elif series_id in line:
            cleaned_line = [value.strip() for value in line.split('\t')]
            filtered_lines.append(cleaned_line)
    
    if filtered_lines:
        df = pd.DataFrame(filtered_lines, columns=header)
        print(f'Datos cargados correctamente para la serie {series_id}')
        return df

df_women = download_and_process(data_url, series['women_gov'])
df_prod = download_and_process(data_url, series['prod_nonsuper_employ'])
df_all_employees = download_and_process(data_url, series['all_employees'])

params = {
    'dbname': 'employment_analysis',
    'user': 'postgres',
    'password': os.environ.get('POSTGRES_PASSWORD', 'Pache939608'),  # Usar variable de entorno
    'host': 'localhost',
    'port': '5432'
}

try:
    conn = psycopg2.connect(**params)
    cur = conn.cursor()

    # Crear la tabla women_gov
    cur.execute("""
    CREATE TABLE IF NOT EXISTS women_gov (
        id SERIAL PRIMARY KEY,
        series_id VARCHAR(255),
        year INT,
        period VARCHAR(255),
        value INTEGER,
        footnote_codes VARCHAR(255)
    );
    """)

    # Crear la tabla prod_employees si no existe
    cur.execute("""
    CREATE TABLE IF NOT EXISTS prod_employees (
        id SERIAL PRIMARY KEY,
        series_id VARCHAR(255),
        year INT,
        period VARCHAR(255),
        value INTEGER,
        footnote_codes VARCHAR(255)
    );
    """)

    # Crear la tabla all_employees si no existe
    cur.execute("""
    CREATE TABLE IF NOT EXISTS all_employees (
        id SERIAL PRIMARY KEY,
        series_id VARCHAR(255),
        year INT,
        period VARCHAR(255),
        value INTEGER,
        footnote_codes VARCHAR(255)
    );
    """)

    # Confirmar las transacciones
    conn.commit()

    # Insertar datos del DataFrame df_women en la tabla women_gov
    for index, row in df_women.iterrows():
        cur.execute("""
        INSERT INTO women_gov (series_id, year, period, value, footnote_codes)
        VALUES (%s, %s, %s, %s, %s);
        """, (row['series_id'], row['year'], row['period'], row['value'], row['footnote_codes']))

    # Insertar datos del DataFrame df_prod en la tabla prod_employees
    for index, row in df_prod.iterrows():
        cur.execute("""
        INSERT INTO prod_employees (series_id, year, period, value, footnote_codes)
        VALUES (%s, %s, %s, %s, %s);
        """, (row['series_id'], row['year'], row['period'], row['value'], row['footnote_codes']))

    # Insertar datos del DataFrame df_all_employee en la tabla all_employees
    for index, row in df_all_employees.iterrows():
        cur.execute("""
        INSERT INTO all_employees (series_id, year, period, value, footnote_codes)
        VALUES (%s, %s, %s, %s, %s);
        """, (row['series_id'], row['year'], row['period'], row['value'], row['footnote_codes']))

    # Confirmar las transacciones
    conn.commit()

    # Crear la vista women_in_government
    sql_create_view = """
    CREATE OR REPLACE VIEW women_in_government AS
    SELECT 
        CASE 
            WHEN period = 'M01' THEN 'January'
            WHEN period = 'M02' THEN 'February'
            WHEN period = 'M03' THEN 'March'
            WHEN period = 'M04' THEN 'April'
            WHEN period = 'M05' THEN 'May'
            WHEN period = 'M06' THEN 'June'
            WHEN period = 'M07' THEN 'July'
            WHEN period = 'M08' THEN 'August'
            WHEN period = 'M09' THEN 'September'
            WHEN period = 'M10' THEN 'October'
            WHEN period = 'M11' THEN 'November'
            WHEN period = 'M12' THEN 'December'
        END || ' ' || year AS date,
        value AS valueInThousands
    FROM 
        women_gov
    ORDER BY 
        year, period;
    """

    cur.execute(sql_create_view)
    conn.commit()
    print("Vista women_in_government creada con éxito.")

    # Crear la vista supervisor_employees
    sql_create_view = """
    CREATE VIEW supervisor_employees AS
    SELECT 
        a.year, 
        a.period, 
        (a.value - p.value) AS supervisor_value
    FROM 
        all_employees a
    JOIN 
        prod_employees p ON a.year = p.year AND a.period = p.period;
    """
    cur.execute(sql_create_view)
    conn.commit()
    print("Vista supervisor_employees creada con éxito.")

    # Crear la vista supervisor_employees
    sql_create_view = """
    -- Crear la vista con la definición correcta
    CREATE VIEW prod_supervisor_ratio AS
    SELECT 
        CASE 
            WHEN p.period = 'M01' THEN 'January'
            WHEN p.period = 'M02' THEN 'February'
            WHEN p.period = 'M03' THEN 'March'
            WHEN p.period = 'M04' THEN 'April'
            WHEN p.period = 'M05' THEN 'May'
            WHEN p.period = 'M06' THEN 'June'
            WHEN p.period = 'M07' THEN 'July'
            WHEN p.period = 'M08' THEN 'August'
            WHEN p.period = 'M09' THEN 'September'
            WHEN p.period = 'M10' THEN 'October'
            WHEN p.period = 'M11' THEN 'November'
            WHEN p.period = 'M12' THEN 'December'
        END || ' ' || p.year AS date,
        ROUND((p.value::float / s.supervisor_value)::numeric, 2) AS ratio
    FROM 
        prod_employees p
    JOIN 
        supervisor_employees s ON p.year = s.year AND p.period = s.period
    ORDER BY 
        p.year, p.period;
    """
    cur.execute(sql_create_view)
    conn.commit()
    print("Vista prod_supervisor_ratio creada con éxito.")

    # Cerrar el cursor y la conexión
    cur.close()
    conn.close()

except psycopg2.OperationalError as e:
    print(f"Error al conectar a la base de datos: {e}")
except Exception as e:
    print(f"Error general: {e}")
    if conn:
        conn.rollback()