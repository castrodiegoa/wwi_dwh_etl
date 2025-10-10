import pandas as pd
from sqlalchemy import create_engine
from src.config.config import SQLSERVER_CONFIG


def get_sqlserver_engine():
    user = SQLSERVER_CONFIG["user"]
    password = SQLSERVER_CONFIG["password"]
    host = SQLSERVER_CONFIG["host"]
    port = SQLSERVER_CONFIG["port"]
    database = SQLSERVER_CONFIG["database"]

    params = "driver=ODBC+Driver+18+for+SQL+Server" "&TrustServerCertificate=yes"
    url = f"mssql+pyodbc://{user}:{password}" f"@{host}:{port}/{database}?{params}"
    engine = create_engine(url)
    return engine


# Extraccion para luego llenar la tabla de hechos y la dimension tiempo
def extract_facturas() -> pd.DataFrame:
    engine = get_sqlserver_engine()
    query = """
    SELECT
        I.InvoiceID      AS factura_id,
        I.CustomerID     AS cliente_id,
        I.SalespersonPersonID AS empleado_id,
        L.StockItemID    AS producto_id,
        L.InvoiceLineID  AS detalle_factura_id,
        I.InvoiceDate    AS fecha_operacion,
        L.Quantity       AS cantidad,
        L.UnitPrice      AS precio_unitario,
        L.TaxAmount      AS monto_impuesto,
        L.ExtendedPrice  AS precio_extendido,
        L.LineProfit     AS ganancia_linea
    FROM Sales.Invoices I
    JOIN Sales.InvoiceLines L
      ON I.InvoiceID = L.InvoiceID
    """
    facturas_base = pd.read_sql(query, con=engine)
    return facturas_base


def extract_clientes() -> pd.DataFrame:
    engine = get_sqlserver_engine()
    query = """
    SELECT
        C.CustomerID            AS cliente_id,
        C.CustomerName          AS nombre_cliente,
        CC.CustomerCategoryName AS nombre_categoria,
        CI.CityName             AS ciudad,
        SP.StateProvinceName    AS provincia,
        CO.CountryName          AS pais
    FROM Sales.Customers C
    JOIN Sales.CustomerCategories CC
      ON C.CustomerCategoryID = CC.CustomerCategoryID
    JOIN Application.Cities CI
      ON C.DeliveryCityID = CI.CityID
    JOIN Application.StateProvinces SP
      ON CI.StateProvinceID = SP.StateProvinceID
    JOIN Application.Countries CO
      ON SP.CountryID = CO.CountryID
    """
    clientes_base = pd.read_sql(query, con=engine)
    return clientes_base


# Extraccion de empleados que sean vendedores
def extract_empleados() -> pd.DataFrame:
    engine = get_sqlserver_engine()
    query = """
    SELECT
        P.PersonID       AS empleado_id,
        P.FullName       AS nombre_empleado,
        P.PreferredName  AS nombre_preferido
    FROM Application.People P
    WHERE P.IsSalesperson = 1
    """
    empleados_base = pd.read_sql(query, con=engine)
    return empleados_base


def extract_productos() -> pd.DataFrame:
    engine = get_sqlserver_engine()
    query = """
    SELECT
        S.StockItemID       AS producto_id,
        S.StockItemName     AS nombre_producto,
        S.Size              AS tamaño,
        S.Brand             AS marca,
        S.IsChillerStock    AS es_refrigerado,
        PT.PackageTypeName  AS tipo_paquete
    FROM Warehouse.StockItems S
    JOIN Warehouse.PackageTypes PT
      ON S.UnitPackageID = PT.PackageTypeID
    """
    productos_base = pd.read_sql(query, con=engine)
    return productos_base


def extract_proveedores() -> pd.DataFrame:
    engine = get_sqlserver_engine()
    query = """
    SELECT
        S.SupplierID            AS proveedor_id,
        S.SupplierName          AS nombre_proveedor,
        SC.SupplierCategoryName AS nombre_categoria,
        CI.CityName             AS ciudad,
        SP.StateProvinceName    AS provincia,
        CO.CountryName          AS pais
    FROM Purchasing.Suppliers S
    JOIN Purchasing.SupplierCategories SC
      ON S.SupplierCategoryID = SC.SupplierCategoryID
    JOIN Application.Cities CI
      ON S.DeliveryCityID = CI.CityID
    JOIN Application.StateProvinces SP
      ON CI.StateProvinceID = SP.StateProvinceID
    JOIN Application.Countries CO
      ON SP.CountryID = CO.CountryID
    """
    proveedores_base = pd.read_sql(query, con=engine)
    return proveedores_base
