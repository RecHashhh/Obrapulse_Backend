from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from app.services.pac_service import (
    obtener_pac,
    obtener_kpis,
    obtener_top_provincias,
    obtener_top_ciudades,
    obtener_top_entidades,
    obtener_top_procedimientos,
    obtener_distribucion_tipo_compra,
    obtener_distribucion_procedimiento,
    obtener_evolucion_fecha,
    obtener_histograma_montos,
    obtener_catalogos_dinamicos,
    obtener_dashboard_contextual,
    obtener_top_entidades_por_provincia,
    obtener_entidades_por_provincia,
    exportar_pac_csv,
    exportar_pac_excel,
)

router = APIRouter()


def common_filters(
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    return {
        "entidad": entidad,
        "provincia": provincia,
        "ciudad": ciudad,
        "tipo_compra": tipo_compra,
        "procedimiento": procedimiento,
        "fecha_inicio": fecha_inicio,
        "fecha_fin": fecha_fin,
        "valor_min": valor_min,
        "valor_max": valor_max,
    }


@router.get("")
def listar_pac(
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
):
    return obtener_pac(
        entidad=entidad,
        provincia=provincia,
        ciudad=ciudad,
        tipo_compra=tipo_compra,
        procedimiento=procedimiento,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        valor_min=valor_min,
        valor_max=valor_max,
        page=page,
        page_size=page_size,
    )


@router.get("/kpis")
def kpis(
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    return obtener_kpis(**common_filters(
        entidad, provincia, ciudad, tipo_compra, procedimiento,
        fecha_inicio, fecha_fin, valor_min, valor_max
    ))


@router.get("/top-provincias")
def top_provincias(
    limit: int = Query(10, ge=1, le=50),
    metrica: str = Query("monto", pattern="^(monto|registros)$"),
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    return obtener_top_provincias(
        limit=limit,
        metrica=metrica,
        **common_filters(
            entidad, provincia, ciudad, tipo_compra, procedimiento,
            fecha_inicio, fecha_fin, valor_min, valor_max
        )
    )


@router.get("/top-ciudades")
def top_ciudades(
    limit: int = Query(10, ge=1, le=50),
    metrica: str = Query("monto", pattern="^(monto|registros)$"),
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    return obtener_top_ciudades(
        limit=limit,
        metrica=metrica,
        **common_filters(
            entidad, provincia, ciudad, tipo_compra, procedimiento,
            fecha_inicio, fecha_fin, valor_min, valor_max
        )
    )


@router.get("/top-entidades")
def top_entidades(
    limit: int = Query(10, ge=1, le=50),
    metrica: str = Query("monto", pattern="^(monto|registros)$"),
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    return obtener_top_entidades(
        limit=limit,
        metrica=metrica,
        **common_filters(
            entidad, provincia, ciudad, tipo_compra, procedimiento,
            fecha_inicio, fecha_fin, valor_min, valor_max
        )
    )


@router.get("/top-procedimientos")
def top_procedimientos(
    limit: int = Query(10, ge=1, le=50),
    metrica: str = Query("monto", pattern="^(monto|registros)$"),
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    return obtener_top_procedimientos(
        limit=limit,
        metrica=metrica,
        **common_filters(
            entidad, provincia, ciudad, tipo_compra, procedimiento,
            fecha_inicio, fecha_fin, valor_min, valor_max
        )
    )


@router.get("/distribucion-tipo-compra")
def distribucion_tipo_compra(
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    return obtener_distribucion_tipo_compra(**common_filters(
        entidad, provincia, ciudad, tipo_compra, procedimiento,
        fecha_inicio, fecha_fin, valor_min, valor_max
    ))


@router.get("/distribucion-procedimiento")
def distribucion_procedimiento(
    limit: int = Query(10, ge=1, le=50),
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    return obtener_distribucion_procedimiento(
        limit=limit,
        **common_filters(
            entidad, provincia, ciudad, tipo_compra, procedimiento,
            fecha_inicio, fecha_fin, valor_min, valor_max
        )
    )


@router.get("/evolucion-fecha")
def evolucion_fecha(
    metrica: str = Query("registros", pattern="^(monto|registros)$"),
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    return obtener_evolucion_fecha(
        metrica=metrica,
        **common_filters(
            entidad, provincia, ciudad, tipo_compra, procedimiento,
            fecha_inicio, fecha_fin, valor_min, valor_max
        )
    )


@router.get("/histograma-montos")
def histograma_montos(
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    return obtener_histograma_montos(**common_filters(
        entidad, provincia, ciudad, tipo_compra, procedimiento,
        fecha_inicio, fecha_fin, valor_min, valor_max
    ))


@router.get("/catalogos-dinamicos")
def catalogos_dinamicos(
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    return obtener_catalogos_dinamicos(**common_filters(
        entidad, provincia, ciudad, tipo_compra, procedimiento,
        fecha_inicio, fecha_fin, valor_min, valor_max
    ))


@router.get("/dashboard-contextual")
def dashboard_contextual(
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
    metrica: str = Query("monto", pattern="^(monto|registros)$"),
    view: str = Query("all", pattern="^(all|dashboard|territorial|temporal)$"),
):
    return obtener_dashboard_contextual(
        metrica=metrica,
        view=view,
        **common_filters(
            entidad, provincia, ciudad, tipo_compra, procedimiento,
            fecha_inicio, fecha_fin, valor_min, valor_max
        )
    )


@router.get("/top-entidades-por-provincia")
def top_entidades_por_provincia(
    provincia: str,
    limit: int = Query(6, ge=1, le=50),
    capa: str = Query("monto", pattern="^(monto|contratos|promedio)$"),
    entidad: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    return obtener_top_entidades_por_provincia(
        provincia=provincia,
        limit=limit,
        capa=capa,
        entidad=entidad,
        ciudad=ciudad,
        tipo_compra=tipo_compra,
        procedimiento=procedimiento,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        valor_min=valor_min,
        valor_max=valor_max,
    )


@router.get("/entidades-por-provincia")
def entidades_por_provincia(
    provincia: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    capa: str = Query("monto", pattern="^(monto|contratos|promedio)$"),
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    return obtener_entidades_por_provincia(
        provincia=provincia,
        page=page,
        page_size=page_size,
        capa=capa,
        ciudad=ciudad,
        tipo_compra=tipo_compra,
        procedimiento=procedimiento,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin,
        valor_min=valor_min,
        valor_max=valor_max,
    )

@router.get("/export/csv")
def export_csv(
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    buffer = exportar_pac_csv(**common_filters(
        entidad, provincia, ciudad, tipo_compra, procedimiento,
        fecha_inicio, fecha_fin, valor_min, valor_max
    ))

    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename=PAC_Export_{provincia or 'general'}.csv"
        }
    )


@router.get("/export/excel")
def export_excel(
    entidad: str = None,
    provincia: str = None,
    ciudad: str = None,
    tipo_compra: str = None,
    procedimiento: str = None,
    fecha_inicio: str = None,
    fecha_fin: str = None,
    valor_min: float = None,
    valor_max: float = None,
):
    output = exportar_pac_excel(**common_filters(
        entidad, provincia, ciudad, tipo_compra, procedimiento,
        fecha_inicio, fecha_fin, valor_min, valor_max
    ))

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename=PAC_Export_{provincia or 'general'}.xlsx"
        }
    )