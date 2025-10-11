from src.extract.extract_wwi import (
    extract_facturas,
    extract_clientes,
    extract_empleados,
    extract_productos,
    extract_proveedores,
)
from src.transform.transform_data import (
    build_dim_tiempo,
    build_dim_cliente,
    build_dim_producto,
    build_dim_empleado,
    build_dim_proveedor,
    build_fact_ventas,
)
from src.load.load_postgres import load_to_postgres


def main():
    # EXTRACCIÓN
    facturas_base = extract_facturas()
    cliente_base = extract_clientes()
    empleado_base = extract_empleados()
    producto_base = extract_productos()
    proveedor_base = extract_proveedores()

    # TRANSFORMACIÓN
    dim_tiempo = build_dim_tiempo(facturas_base)
    dim_cliente = build_dim_cliente(cliente_base)
    dim_empleado = build_dim_empleado(empleado_base)
    dim_producto = build_dim_producto(producto_base)
    dim_proveedor = build_dim_proveedor(proveedor_base)
    fact_ventas = build_fact_ventas(
        facturas_base,
        dim_tiempo,
        dim_cliente,
        dim_producto,
        dim_empleado,
        dim_proveedor,
    )

    # CARGA
    load_to_postgres(dim_tiempo, "dim_tiempo")
    load_to_postgres(dim_cliente, "dim_cliente")
    load_to_postgres(dim_producto, "dim_producto")
    load_to_postgres(dim_empleado, "dim_empleado")
    load_to_postgres(dim_proveedor, "dim_proveedor")
    load_to_postgres(fact_ventas, "fact_ventas")

    print("ETL completado.")


if __name__ == "__main__":
    main()
