import io
import pandas as pd
from sqlalchemy import text
from app.db import get_engine

engine = get_engine()


def _build_filters(
    entidad=None,
    provincia=None,
    ciudad=None,
    tipo_compra=None,
    procedimiento=None,
    t_regimen=None,
    fondo_bid=None,
    fecha_inicio=None,
    fecha_fin=None,
    valor_min=None,
    valor_max=None,
):
    filtros = []
    params = {}

    if entidad:
        filtros.append("Entidad = :entidad")
        params["entidad"] = entidad

    if provincia:
        filtros.append("Provincia = :provincia")
        params["provincia"] = provincia

    if ciudad:
        filtros.append("Ciudad = :ciudad")
        params["ciudad"] = ciudad

    if tipo_compra:
        filtros.append("T_Compra = :tipo_compra")
        params["tipo_compra"] = tipo_compra

    if procedimiento:
        filtros.append("Procedimiento = :procedimiento")
        params["procedimiento"] = procedimiento

    if t_regimen:
        filtros.append("T_Regimen = :t_regimen")
        params["t_regimen"] = t_regimen

    if fondo_bid:
        filtros.append("Fondo_BID = :fondo_bid")
        params["fondo_bid"] = fondo_bid

    if fecha_inicio:
        filtros.append("Fecha_Carga >= :fecha_inicio")
        params["fecha_inicio"] = fecha_inicio

    if fecha_fin:
        filtros.append("Fecha_Carga <= :fecha_fin")
        params["fecha_fin"] = fecha_fin

    if valor_min is not None:
        filtros.append("ISNULL(V_Total_Numeric, 0) >= :valor_min")
        params["valor_min"] = valor_min

    if valor_max is not None:
        filtros.append("ISNULL(V_Total_Numeric, 0) <= :valor_max")
        params["valor_max"] = valor_max

    where_sql = ""
    if filtros:
        where_sql = "WHERE " + " AND ".join(filtros)

    return where_sql, params


def _append_condition(where_sql: str, condition: str) -> str:
    if where_sql:
        return f"{where_sql} AND {condition}"
    return f"WHERE {condition}"


def _run_list_query(sql: str, params: dict | None = None):
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params or {}).mappings().all()
    return [dict(row) for row in rows]


def _run_one_query(sql: str, params: dict | None = None):
    with engine.connect() as conn:
        row = conn.execute(text(sql), params or {}).mappings().first()
    return dict(row) if row else {}


def obtener_pac(
    entidad=None,
    provincia=None,
    ciudad=None,
    tipo_compra=None,
    procedimiento=None,
    t_regimen=None,
    fondo_bid=None,
    fecha_inicio=None,
    fecha_fin=None,
    valor_min=None,
    valor_max=None,
    page=1,
    page_size=20,
):
    where_sql, params = _build_filters(
        entidad=entidad,
        provincia=provincia,
        ciudad=ciudad,
        tipo_compra=tipo_compra,
        procedimiento=procedimiento,
        t_regimen=t_regimen,
        fondo_bid=fondo_bid,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        valor_min=valor_min,
        valor_max=valor_max,
    )

    offset = (page - 1) * page_size
    params["offset"] = offset
    params["page_size"] = page_size

    sql = f"""
    SELECT
        id, Nro, Partida_Pres, CPC, T_Compra, T_Regimen, Fondo_BID,
        Tipo_Presupuesto, Tipo_Producto, Cat_Electronico, Procedimiento,
        Descripcion, Cantidad, Unidad_Medida, Costo_Unitario, V_Total,
        Extra, Periodo, V_Total_Numeric, Tipo_Tabla, Entidad, url,
        Nombre_Comercial, Provincia, Ciudad, Fecha_Carga, hash_registro
    FROM tb.pac_partidas
    {where_sql}
    ORDER BY Fecha_Carga DESC, id DESC
    OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
    """

    count_sql = f"""
    SELECT COUNT(*) AS total
    FROM tb.pac_partidas
    {where_sql}
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
        total = conn.execute(text(count_sql), params).scalar()

    return {
        "total": total or 0,
        "page": page,
        "page_size": page_size,
        "items": [dict(row) for row in rows],
    }


def obtener_kpis(**filters):
    where_sql, params = _build_filters(**filters)

    sql = f"""
    SELECT
        COUNT(*) AS total_registros,
        COUNT(DISTINCT Entidad) AS total_entidades,
        COUNT(DISTINCT Provincia) AS total_provincias,
        COUNT(DISTINCT Ciudad) AS total_ciudades,
        SUM(ISNULL(V_Total_Numeric, 0)) AS monto_total,
        AVG(ISNULL(V_Total_Numeric, 0)) AS promedio_monto,
        MAX(Fecha_Carga) AS ultima_fecha_carga
    FROM tb.pac_partidas
    {where_sql}
    """
    return _run_one_query(sql, params)


def obtener_top_provincias(limit=10, metrica="monto", **filters):
    where_sql, params = _build_filters(**filters)
    params["limit"] = limit
    order_field = "monto_total" if metrica == "monto" else "total_registros"

    sql = f"""
    SELECT TOP (:limit)
        Provincia AS nombre,
        COUNT(*) AS total_registros,
        SUM(ISNULL(V_Total_Numeric, 0)) AS monto_total
    FROM tb.pac_partidas
    {_append_condition(where_sql, "Provincia IS NOT NULL")}
    GROUP BY Provincia
    ORDER BY {order_field} DESC
    """
    return _run_list_query(sql, params)


def obtener_top_ciudades(limit=10, metrica="monto", **filters):
    where_sql, params = _build_filters(**filters)
    params["limit"] = limit
    order_field = "monto_total" if metrica == "monto" else "total_registros"

    sql = f"""
    SELECT TOP (:limit)
        Ciudad AS nombre,
        COUNT(*) AS total_registros,
        SUM(ISNULL(V_Total_Numeric, 0)) AS monto_total
    FROM tb.pac_partidas
    {_append_condition(where_sql, "Ciudad IS NOT NULL")}
    GROUP BY Ciudad
    ORDER BY {order_field} DESC
    """
    return _run_list_query(sql, params)


def obtener_top_entidades(limit=10, metrica="monto", **filters):
    where_sql, params = _build_filters(**filters)
    params["limit"] = limit
    order_field = "monto_total" if metrica == "monto" else "total_registros"

    sql = f"""
    SELECT TOP (:limit)
        Entidad AS nombre,
        COUNT(*) AS total_registros,
        SUM(ISNULL(V_Total_Numeric, 0)) AS monto_total
    FROM tb.pac_partidas
    {_append_condition(where_sql, "Entidad IS NOT NULL")}
    GROUP BY Entidad
    ORDER BY {order_field} DESC
    """
    return _run_list_query(sql, params)


def obtener_top_entidades_por_provincia(
    provincia,
    limit=10,
    capa="monto",
    **filters,
):
    where_sql, params = _build_filters(
        entidad=filters.get("entidad"),
        provincia=provincia,
        ciudad=filters.get("ciudad"),
        tipo_compra=filters.get("tipo_compra"),
        procedimiento=filters.get("procedimiento"),
        t_regimen=filters.get("t_regimen"),
        fondo_bid=filters.get("fondo_bid"),
        fecha_inicio=filters.get("fecha_inicio"),
        fecha_fin=filters.get("fecha_fin"),
        valor_min=filters.get("valor_min"),
        valor_max=filters.get("valor_max"),
    )

    params["limit"] = limit
    capa = (capa or "monto").lower()

    if capa == "contratos":
        order_field = "total_registros"
    elif capa == "promedio":
        order_field = "promedio_contrato"
    else:
        order_field = "monto_total"

    sql = f"""
    SELECT TOP (:limit)
        Entidad AS nombre,
        COUNT(*) AS total_registros,
        SUM(ISNULL(V_Total_Numeric, 0)) AS monto_total,
        AVG(ISNULL(V_Total_Numeric, 0)) AS promedio_contrato
    FROM tb.pac_partidas
    {_append_condition(where_sql, "Entidad IS NOT NULL")}
    GROUP BY Entidad
    ORDER BY {order_field} DESC
    """
    return _run_list_query(sql, params)


def obtener_entidades_por_provincia(
    provincia,
    page=1,
    page_size=20,
    capa="monto",
    **filters,
):
    where_sql, params = _build_filters(
        entidad=None,
        provincia=provincia,
        ciudad=filters.get("ciudad"),
        tipo_compra=filters.get("tipo_compra"),
        procedimiento=filters.get("procedimiento"),
        t_regimen=filters.get("t_regimen"),
        fondo_bid=filters.get("fondo_bid"),
        fecha_inicio=filters.get("fecha_inicio"),
        fecha_fin=filters.get("fecha_fin"),
        valor_min=filters.get("valor_min"),
        valor_max=filters.get("valor_max"),
    )

    offset = (page - 1) * page_size
    params["offset"] = offset
    params["page_size"] = page_size

    capa = (capa or "monto").lower()
    if capa == "contratos":
        order_field = "total_registros"
    elif capa == "promedio":
        order_field = "promedio_contrato"
    else:
        order_field = "monto_total"

    sql = f"""
    WITH entidades AS (
        SELECT
            Entidad AS nombre,
            COUNT(*) AS total_registros,
            SUM(ISNULL(V_Total_Numeric, 0)) AS monto_total,
            AVG(ISNULL(V_Total_Numeric, 0)) AS promedio_contrato
        FROM tb.pac_partidas
        {_append_condition(where_sql, "Entidad IS NOT NULL")}
        GROUP BY Entidad
    )
    SELECT
        nombre,
        total_registros,
        monto_total,
        promedio_contrato
    FROM entidades
    ORDER BY {order_field} DESC, nombre ASC
    OFFSET :offset ROWS FETCH NEXT :page_size ROWS ONLY
    """

    count_sql = f"""
    SELECT COUNT(DISTINCT Entidad) AS total
    FROM tb.pac_partidas
    {_append_condition(where_sql, "Entidad IS NOT NULL")}
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()
        total = conn.execute(text(count_sql), params).scalar()

    return {
        "total": total or 0,
        "page": page,
        "page_size": page_size,
        "items": [dict(row) for row in rows],
    }


def obtener_top_procedimientos(limit=10, metrica="monto", **filters):
    where_sql, params = _build_filters(**filters)
    params["limit"] = limit
    order_field = "monto_total" if metrica == "monto" else "total_registros"

    sql = f"""
    SELECT TOP (:limit)
        Procedimiento AS nombre,
        COUNT(*) AS total_registros,
        SUM(ISNULL(V_Total_Numeric, 0)) AS monto_total
    FROM tb.pac_partidas
    {_append_condition(where_sql, "Procedimiento IS NOT NULL")}
    GROUP BY Procedimiento
    ORDER BY {order_field} DESC
    """
    return _run_list_query(sql, params)


def obtener_distribucion_tipo_compra(**filters):
    where_sql, params = _build_filters(**filters)

    sql = f"""
    SELECT
        T_Compra,
        COUNT(*) AS total_registros,
        SUM(ISNULL(V_Total_Numeric, 0)) AS monto_total
    FROM tb.pac_partidas
    {_append_condition(where_sql, "T_Compra IS NOT NULL")}
    GROUP BY T_Compra
    ORDER BY total_registros DESC
    """
    return _run_list_query(sql, params)


def obtener_distribucion_procedimiento(limit=10, **filters):
    where_sql, params = _build_filters(**filters)
    params["limit"] = limit

    sql = f"""
    SELECT TOP (:limit)
        Procedimiento,
        COUNT(*) AS total_registros,
        SUM(ISNULL(V_Total_Numeric, 0)) AS monto_total
    FROM tb.pac_partidas
    {_append_condition(where_sql, "Procedimiento IS NOT NULL")}
    GROUP BY Procedimiento
    ORDER BY total_registros DESC
    """
    return _run_list_query(sql, params)


def obtener_evolucion_fecha(metrica="registros", **filters):
    where_sql, params = _build_filters(**filters)

    if metrica == "monto":
        select_metric = "SUM(ISNULL(V_Total_Numeric, 0)) AS valor"
    else:
        select_metric = "COUNT(*) AS valor"

    sql = f"""
    SELECT
        Fecha_Carga,
        {select_metric}
    FROM tb.pac_partidas
    {_append_condition(where_sql, "Fecha_Carga IS NOT NULL")}
    GROUP BY Fecha_Carga
    ORDER BY Fecha_Carga ASC
    """
    return _run_list_query(sql, params)


def obtener_histograma_montos(**filters):
    where_sql, params = _build_filters(**filters)

    sql = f"""
    SELECT
        CASE
            WHEN ISNULL(V_Total_Numeric, 0) < 10000 THEN '0 - 10 mil'
            WHEN ISNULL(V_Total_Numeric, 0) < 100000 THEN '10 mil - 100 mil'
            WHEN ISNULL(V_Total_Numeric, 0) < 1000000 THEN '100 mil - 1 millón'
            ELSE 'Más de 1 millón'
        END AS rango,
        COUNT(*) AS total_registros,
        SUM(ISNULL(V_Total_Numeric, 0)) AS monto_total
    FROM tb.pac_partidas
    {where_sql}
    GROUP BY
        CASE
            WHEN ISNULL(V_Total_Numeric, 0) < 10000 THEN '0 - 10 mil'
            WHEN ISNULL(V_Total_Numeric, 0) < 100000 THEN '10 mil - 100 mil'
            WHEN ISNULL(V_Total_Numeric, 0) < 1000000 THEN '100 mil - 1 millón'
            ELSE 'Más de 1 millón'
        END
    ORDER BY monto_total DESC
    """
    return _run_list_query(sql, params)


def obtener_catalogos_dinamicos(**filters):
    provincia = filters.get("provincia")
    ciudad = filters.get("ciudad")
    tipo_compra = filters.get("tipo_compra")

    where_sql, params = _build_filters(
        entidad=None,
        provincia=provincia,
        ciudad=ciudad,
        tipo_compra=tipo_compra,
        procedimiento=filters.get("procedimiento"),
        t_regimen=filters.get("t_regimen"),
        fondo_bid=filters.get("fondo_bid"),
        fecha_inicio=filters.get("fecha_inicio"),
        fecha_fin=filters.get("fecha_fin"),
        valor_min=filters.get("valor_min"),
        valor_max=filters.get("valor_max"),
    )

    with engine.connect() as conn:
        provincias = conn.execute(text("""
            SELECT DISTINCT Provincia
            FROM tb.pac_partidas
            WHERE Provincia IS NOT NULL
            ORDER BY Provincia
        """)).fetchall()

        ciudades_sql = f"""
            SELECT DISTINCT Ciudad
            FROM tb.pac_partidas
            {_append_condition(where_sql if provincia else "", "Ciudad IS NOT NULL")}
            ORDER BY Ciudad
        """
        ciudades = conn.execute(text(ciudades_sql), params).fetchall()

        entidades_where, entidades_params = _build_filters(
            provincia=provincia,
            ciudad=ciudad,
            fecha_inicio=filters.get("fecha_inicio"),
            fecha_fin=filters.get("fecha_fin"),
            t_regimen=filters.get("t_regimen"),
            fondo_bid=filters.get("fondo_bid"),
            valor_min=filters.get("valor_min"),
            valor_max=filters.get("valor_max"),
        )
        entidades_sql = f"""
            SELECT DISTINCT Entidad
            FROM tb.pac_partidas
            {_append_condition(entidades_where, "Entidad IS NOT NULL")}
            ORDER BY Entidad
        """
        entidades = conn.execute(text(entidades_sql), entidades_params).fetchall()

        tipos_sql = f"""
            SELECT DISTINCT T_Compra
            FROM tb.pac_partidas
            {_append_condition(where_sql, "T_Compra IS NOT NULL")}
            ORDER BY T_Compra
        """
        tipos = conn.execute(text(tipos_sql), params).fetchall()

        procedimientos_sql = f"""
            SELECT DISTINCT Procedimiento
            FROM tb.pac_partidas
            {_append_condition(where_sql, "Procedimiento IS NOT NULL")}
            ORDER BY Procedimiento
        """
        procedimientos = conn.execute(text(procedimientos_sql), params).fetchall()

        regimenes_sql = f"""
            SELECT DISTINCT T_Regimen
            FROM tb.pac_partidas
            {_append_condition(where_sql, "T_Regimen IS NOT NULL")}
            ORDER BY T_Regimen
        """
        regimenes = conn.execute(text(regimenes_sql), params).fetchall()

        fondos_sql = f"""
            SELECT DISTINCT Fondo_BID
            FROM tb.pac_partidas
            {_append_condition(where_sql, "Fondo_BID IS NOT NULL")}
            ORDER BY Fondo_BID
        """
        fondos = conn.execute(text(fondos_sql), params).fetchall()

    return {
        "provincias": [r[0] for r in provincias],
        "ciudades": [r[0] for r in ciudades],
        "entidades": [r[0] for r in entidades],
        "tipos_compra": [r[0] for r in tipos],
        "procedimientos": [r[0] for r in procedimientos],
        "regimenes": [r[0] for r in regimenes],
        "fondos_bid": [r[0] for r in fondos],
    }


def obtener_dashboard_contextual(metrica="monto", view="all", **filters):
    has_provincia = bool(filters.get("provincia"))
    has_ciudad = bool(filters.get("ciudad"))
    has_entidad = bool(filters.get("entidad"))

    if has_entidad:
        nivel = "entidad"
        principal = {
            "titulo": "Top procedimientos",
            "tipo": "top_procedimientos",
            "data": obtener_top_procedimientos(limit=10, metrica=metrica, **filters),
        }
    elif has_ciudad:
        nivel = "ciudad"
        principal = {
            "titulo": "Top entidades",
            "tipo": "top_entidades",
            "data": obtener_top_entidades(limit=10, metrica=metrica, **filters),
        }
    elif has_provincia:
        nivel = "provincia"
        principal = {
            "titulo": "Top ciudades",
            "tipo": "top_ciudades",
            "data": obtener_top_ciudades(limit=10, metrica=metrica, **filters),
        }
    else:
        nivel = "global"
        principal = {
            "titulo": "Top provincias",
            "tipo": "top_provincias",
            "data": obtener_top_provincias(limit=10, metrica=metrica, **filters),
        }

    complementario = None
    if nivel == "global":
        complementario = {
            "titulo": "Top entidades",
            "tipo": "top_entidades",
            "data": obtener_top_entidades(limit=10, metrica=metrica, **filters),
        }
    elif nivel == "provincia":
        complementario = {
            "titulo": "Top entidades",
            "tipo": "top_entidades",
            "data": obtener_top_entidades(limit=10, metrica=metrica, **filters),
        }
    elif nivel == "ciudad":
        complementario = {
            "titulo": "Top procedimientos",
            "tipo": "top_procedimientos",
            "data": obtener_top_procedimientos(limit=10, metrica=metrica, **filters),
        }

    payload = {
        "nivel": nivel,
        "metrica": metrica,
        "kpis": obtener_kpis(**filters),
        "principal": principal,
        "complementario": complementario,
        "tipo_compra": obtener_distribucion_tipo_compra(**filters),
        "procedimientos": obtener_distribucion_procedimiento(limit=10, **filters),
        "evolucion": obtener_evolucion_fecha(
            metrica=metrica if metrica == "monto" else "registros",
            **filters
        ),
        "histograma": obtener_histograma_montos(**filters),
    }

    if view == "dashboard":
        return {
            "nivel": payload["nivel"],
            "metrica": payload["metrica"],
            "kpis": payload["kpis"],
            "principal": payload["principal"],
            "complementario": payload["complementario"],
            "tipo_compra": payload["tipo_compra"],
            "procedimientos": payload["procedimientos"],
            "evolucion": payload["evolucion"],
            "histograma": payload["histograma"],
        }

    if view == "territorial":
        return {
            "nivel": payload["nivel"],
            "metrica": payload["metrica"],
            "kpis": payload["kpis"],
            "principal": payload["principal"],
            "complementario": payload["complementario"],
        }

    if view == "temporal":
        return {
            "nivel": payload["nivel"],
            "metrica": payload["metrica"],
            "kpis": payload["kpis"],
            "evolucion": payload["evolucion"],
            "histograma": payload["histograma"],
        }

    return payload

def obtener_pac_exportable(**filters):
    where_sql, params = _build_filters(**filters)

    sql = f"""
    SELECT
        Nro,
        Partida_Pres,
        CPC,
        T_Compra,
        T_Regimen,
        Fondo_BID,
        Tipo_Presupuesto,
        Tipo_Producto,
        Cat_Electronico,
        Procedimiento,
        Descripcion,
        Cantidad,
        Unidad_Medida,
        Costo_Unitario,
        V_Total,
        Extra,
        Periodo,
        V_Total_Numeric,
        Tipo_Tabla,
        Entidad,
        url,
        Nombre_Comercial,
        Provincia,
        Ciudad,
        Fecha_Carga
    FROM tb.pac_partidas
    {where_sql}
    ORDER BY Fecha_Carga DESC, Entidad ASC
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    return [dict(row) for row in rows]


def exportar_pac_csv(**filters):
    data = obtener_pac_exportable(**filters)
    df = pd.DataFrame(data)

    if df.empty:
        df = pd.DataFrame(columns=[
            "Nro", "Partida_Pres", "CPC", "T_Compra", "T_Regimen", "Fondo_BID",
            "Tipo_Presupuesto", "Tipo_Producto", "Cat_Electronico", "Procedimiento",
            "Descripcion", "Cantidad", "Unidad_Medida", "Costo_Unitario", "V_Total",
            "Extra", "Periodo", "V_Total_Numeric", "Tipo_Tabla", "Entidad", "url",
            "Nombre_Comercial", "Provincia", "Ciudad", "Fecha_Carga"
        ])
    # Remove Tipo_Tabla from export columns if present
    df = df.drop(columns=["Tipo_Tabla"], errors="ignore")
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, encoding="utf-8-sig")
    buffer.seek(0)
    return buffer


def exportar_pac_excel(**filters):
    data = obtener_pac_exportable(**filters)
    df = pd.DataFrame(data)

    if df.empty:
        df = pd.DataFrame(columns=[
            "Nro", "Partida_Pres", "CPC", "T_Compra", "T_Regimen", "Fondo_BID",
            "Tipo_Presupuesto", "Tipo_Producto", "Cat_Electronico", "Procedimiento",
            "Descripcion", "Cantidad", "Unidad_Medida", "Costo_Unitario", "V_Total",
            "Extra", "Periodo", "V_Total_Numeric", "Tipo_Tabla", "Entidad", "url",
            "Nombre_Comercial", "Provincia", "Ciudad", "Fecha_Carga"
        ])
    # Remove Tipo_Tabla from export columns if present
    df = df.drop(columns=["Tipo_Tabla"], errors="ignore")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="PAC")
    output.seek(0)
    return output