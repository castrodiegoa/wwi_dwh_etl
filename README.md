# WWI -> ETL -> DWH

## 🛠️ Setup

1. Clona este repositorio.

2. Crea y activa tu entorno virtual.

    - En **Linux/MacOS**:
        ```bash
        python -m venv .venv
        source .venv/bin/activate
        ```

    - En **Windows**:
        ```bash
        python -m venv .venv
        .\venv\Scripts\activate
        ```

3. Copia el archivo `.env.example` a `.env`y actualiza con tus valores locales.
    ```bash
    cp .env.example .env
    ```

4. Instala las dependencias de Python.
    ```bash
    pip install -r requirements.txt
    ```

## 🏃‍♀️ Ejecución del ETL

1. Una vez configurado el entorno y las variables de entorno, ejecuta el proceso ETL con:
    ```bash
    python main.py
    ```

    Esto realizará el siguiente flujo:
    - **Extract**: Conecta a SQL Server y extrae los datos de las tablas fuente.
    - **Transform**: Procesa los datos con Pandas para crear las dimensiones.
    - **Load**: Carga las tablas transformadas en la base de datos PostgreSQL.
    