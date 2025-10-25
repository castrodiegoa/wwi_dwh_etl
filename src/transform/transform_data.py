import pandas as pd
import numpy as np


def build_dim_tiempo(facturas_base: pd.DataFrame) -> pd.DataFrame:
    """
    Crea la dimensión de tiempo a partir del atributo InvoiceDate en la tabla Invoices.
    """

    # Extraer las fechas y eliminar nulos
    fechas = facturas_base["fecha_operacion"].dropna()
    # Asegurar de convertir a datetime
    fechas = pd.to_datetime(fechas)

    # Obtener el rango de fechas y eliminar horas
    fecha_min = fechas.min().normalize()
    fecha_max = fechas.max().normalize()

    # Generar rango diario (inclusive)
    fechas_range = pd.date_range(start=fecha_min, end=fecha_max, freq="D")
    # Construir un DataFrame base con esas fechas
    dim_tiempo = pd.DataFrame({"fecha_completa": fechas_range})

    # Crear atributos de fecha
    dim_tiempo["dia"] = dim_tiempo["fecha_completa"].dt.day
    dim_tiempo["mes"] = dim_tiempo["fecha_completa"].dt.month
    dim_tiempo["año"] = dim_tiempo["fecha_completa"].dt.year
    dim_tiempo["nombre_dia"] = dim_tiempo["fecha_completa"].dt.day_name(locale="es_ES")
    dim_tiempo["nombre_mes"] = dim_tiempo["fecha_completa"].dt.month_name(
        locale="es_ES"
    )
    dim_tiempo["dia_semana"] = dim_tiempo["fecha_completa"].dt.weekday + 1
    dim_tiempo["semestre"] = np.where(dim_tiempo["mes"] <= 6, 1, 2)
    dim_tiempo["trimestre"] = dim_tiempo["fecha_completa"].dt.quarter
    dim_tiempo["es_fin_semana"] = dim_tiempo["fecha_completa"].dt.weekday >= 5
    dim_tiempo["es_fin_semana"] = (
        dim_tiempo["es_fin_semana"].map({True: "Sí", False: "No"}).fillna("Desconocido")
    )

    # Ordenar las fechas cronológicamente y reiniciar el índice
    dim_tiempo = dim_tiempo.sort_values("fecha_completa").reset_index(drop=True)
    # Generar llave subrogada
    dim_tiempo["tiempo_id"] = dim_tiempo.index + 1

    # Orden final de columnas
    dim_tiempo = dim_tiempo[
        [
            "tiempo_id",
            "fecha_completa",
            "dia",
            "mes",
            "año",
            "nombre_dia",
            "nombre_mes",
            "dia_semana",
            "semestre",
            "trimestre",
            "es_fin_semana",
        ]
    ]

    return dim_tiempo


def build_dim_cliente(clientes_base: pd.DataFrame) -> pd.DataFrame:
    dim_cliente = clientes_base.copy()
    # Eliminar registros sin ID de cliente
    dim_cliente = dim_cliente.dropna(subset=["cliente_id"])
    # Asegurar clientes únicos
    dim_cliente = dim_cliente.drop_duplicates(subset=["cliente_id"])

    # Lista de columnas donde reemplazar nulos
    columnas_texto = [
        "nombre_cliente",
        "nombre_categoria",
        "ciudad",
        "provincia",
        "pais",
    ]

    for col in columnas_texto:
        dim_cliente[col] = dim_cliente[col].fillna("Desconocido")

    # Orden final de columnas
    dim_cliente = dim_cliente[
        [
            "cliente_id",
            "nombre_cliente",
            "nombre_categoria",
            "ciudad",
            "provincia",
            "pais",
        ]
    ]

    return dim_cliente


def build_dim_empleado(empleados_base: pd.DataFrame) -> pd.DataFrame:
    dim_empleado = empleados_base.copy()
    # Eliminar registros sin ID de empleado
    dim_empleado = dim_empleado.dropna(subset=["empleado_id"])
    # Asegurar empleados únicos
    dim_empleado = dim_empleado.drop_duplicates(subset=["empleado_id"])

    # Lista de columnas donde reemplazar nulos
    columnas_texto = [
        "nombre_empleado",
        "nombre_preferido",
    ]

    for col in columnas_texto:
        dim_empleado[col] = dim_empleado[col].fillna("Desconocido")

    # Orden final de columnas
    dim_empleado = dim_empleado[
        [
            "empleado_id",
            "nombre_empleado",
            "nombre_preferido",
        ]
    ]

    return dim_empleado


def build_dim_producto(producto_base: pd.DataFrame) -> pd.DataFrame:
    dim_producto = producto_base.copy()
    # Eliminar registros sin ID de producto
    dim_producto = dim_producto.dropna(subset=["producto_id"])
    # Asegurar productos únicos
    dim_producto = dim_producto.drop_duplicates(subset=["producto_id"])

    # Lista de columnas donde reemplazar nulos
    columnas_texto = [
        "nombre_producto",
        "tamaño",
        "marca",
        "tipo_paquete",
    ]

    for col in columnas_texto:
        dim_producto[col] = dim_producto[col].fillna("Desconocido")

    # Convertir el bit a texto
    dim_producto["es_refrigerado"] = (
        dim_producto["es_refrigerado"]
        .map({True: "Sí", False: "No"})
        .fillna("Desconocido")
    )

    # Orden final de columnas
    dim_producto = dim_producto[
        [
            "producto_id",
            "nombre_producto",
            "tamaño",
            "marca",
            "es_refrigerado",
            "tipo_paquete",
            "proveedor_id",
        ]
    ]

    return dim_producto


def build_dim_proveedor(proveedores_base: pd.DataFrame) -> pd.DataFrame:
    dim_proveedor = proveedores_base.copy()
    # Eliminar registros sin ID de proveedor
    dim_proveedor = dim_proveedor.dropna(subset=["proveedor_id"])
    # Asegurar proveedores únicos
    dim_proveedor = dim_proveedor.drop_duplicates(subset=["proveedor_id"])

    # Lista de columnas donde reemplazar nulos
    columnas_texto = [
        "nombre_proveedor",
        "nombre_categoria",
        "ciudad",
        "provincia",
        "pais",
    ]

    for col in columnas_texto:
        dim_proveedor[col] = dim_proveedor[col].fillna("Desconocido")

    # Orden final de columnas
    dim_proveedor = dim_proveedor[
        [
            "proveedor_id",
            "nombre_proveedor",
            "nombre_categoria",
            "ciudad",
            "provincia",
            "pais",
        ]
    ]

    return dim_proveedor


def build_fact_ventas(
    facturas_base: pd.DataFrame,
    dim_tiempo: pd.DataFrame,
    dim_cliente: pd.DataFrame,
    dim_producto: pd.DataFrame,
    dim_empleado: pd.DataFrame,
    dim_proveedor: pd.DataFrame,
) -> pd.DataFrame:
    """
    Crea la tabla de hechos de ventas AGREGADA a partir de la tabla base
    de facturas y líneas de factura, y las dimensiones.

    Las métricas son a nivel de producto vendido con la combinación:
    día + producto + cliente + empleado + proveedor.
    """

    fact_ventas = facturas_base.copy()
    fact_ventas["fecha_operacion"] = pd.to_datetime(fact_ventas["fecha_operacion"])

    # MERGE de hechos con dim_producto para obtener proveedor_id
    fact_ventas = fact_ventas.merge(
        dim_producto[["producto_id", "proveedor_id"]],
        on="producto_id",
        how="left",
    )

    # MERGE de hechos con dim_tiempo para obtener tiempo_id
    fact_ventas = fact_ventas.merge(
        dim_tiempo[["tiempo_id", "fecha_completa"]],
        left_on="fecha_operacion",
        right_on="fecha_completa",
        how="left",
    )

    # VALIDAR INTEGRIDAD REFERENCIAL
    # Filtrar solo clientes que existen en dim_cliente
    clientes_validos = set(dim_cliente["cliente_id"].values)
    fact_ventas = fact_ventas[fact_ventas["cliente_id"].isin(clientes_validos)]
    # Filtrar solo productos que existen en dim_producto
    productos_validos = set(dim_producto["producto_id"].values)
    fact_ventas = fact_ventas[fact_ventas["producto_id"].isin(productos_validos)]
    # Filtrar solo empleados que existen en dim_empleado
    empleados_validos = set(dim_empleado["empleado_id"].values)
    fact_ventas = fact_ventas[fact_ventas["empleado_id"].isin(empleados_validos)]
    # Filtrar solo proveedores que existen en dim_proveedor
    proveedores_validos = set(dim_proveedor["proveedor_id"].values)
    fact_ventas = fact_ventas[fact_ventas["proveedor_id"].isin(proveedores_validos)]
    # Filtrar solo fechas que existen en dim_tiempo
    tiempos_validos = set(dim_tiempo["tiempo_id"].values)
    fact_ventas = fact_ventas[fact_ventas["tiempo_id"].isin(tiempos_validos)]

    # AGREGACIÓN
    fact_ventas = (
        fact_ventas.groupby(
            ["tiempo_id", "producto_id", "cliente_id", "empleado_id", "proveedor_id"],
            dropna=False,  # Mantener registros aunque haya algún NULL
        )
        .agg(
            {
                # Métricas agregadas (sumatorias)
                "cantidad": "sum",  # -> cantidad_total_vendida
                "precio_unitario": "sum",  # -> monto_ventas_bruto (sin impuestos)
                "monto_impuesto": "sum",  # -> monto_impuestos
                "precio_extendido": "sum",  # -> monto_ventas_neto (con impuestos)
                "ganancia_linea": "sum",  # -> ganancia_total
            }
        )
        .reset_index()
    )

    # Renombrar columnas
    fact_ventas = fact_ventas.rename(
        columns={
            "cantidad": "cantidad_total_vendida",
            "precio_unitario": "monto_ventas_bruto",
            "monto_impuesto": "monto_impuestos",
            "precio_extendido": "monto_ventas_neto",
            "ganancia_linea": "ganancia_total",
        }
    )

    # Generar llave subrogada
    fact_ventas.insert(0, "venta_id", range(1, len(fact_ventas) + 1))

    # Orden final de columnas
    fact_ventas = fact_ventas[
        [
            # FORÁNEAS
            "venta_id",
            "tiempo_id",
            "producto_id",
            "cliente_id",
            "empleado_id",
            "proveedor_id",
            # MÉTRICAS
            "cantidad_total_vendida",  # SUM(cantidad)
            "monto_ventas_bruto",  # SUM(precio_unitario) - sin impuestos
            "monto_impuestos",  # SUM(monto_impuesto)
            "monto_ventas_neto",  # SUM(precio_extendido) - con impuestos
            "ganancia_total",  # SUM(ganancia_linea)
        ]
    ]

    return fact_ventas
