# WWI > ETL > DWH

## 🛠️ Setup

1. Clona este repositorio.

2. Crea y activa un entorno virtual. En la raíz del proyecto:

    ```bash
    python -m venv .venv
    ```

    Ctrl + shift + p y escribir "seleccionar intérprete", elegir el entorno recién creado.

3. Copia el archivo `.env.example` a `.env`y actualiza con tus valores locales.

    ```bash
    cp .env.example .env
    ```

4. En una nueva consola instala las dependencias de Python.

    ```bash
    pip install -r requirements.txt
    ```

Para evitar errores:

- Tener instalado ODBC Driver 18 for SQL Server: <https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver17>
- Tener restaurada la base de datos: <https://learn.microsoft.com/es-es/sql/samples/wide-world-importers-oltp-install-configure?view=sql-server-ver16>

## 🏃‍♀️ Ejecución del ETL

1. Una vez configurado el entorno y las variables de entorno, ejecuta el proceso ETL con:

    ```bash
    python main.py
    ```

    Esto realizará el siguiente flujo:
    - **Extract**: Conecta a SQL Server y extrae los datos de las tablas fuente.
    - **Transform**: Procesa los datos con Pandas para crear las dimensiones.
    - **Load**: Carga las tablas transformadas en la base de datos PostgreSQL.
