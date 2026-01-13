# app.py
import asyncio, json, os
from zoneinfo import ZoneInfo
from fastapi import FastAPI, Body, Request, Query, HTTPException
from fastapi.responses import StreamingResponse, FileResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import mysql.connector
from sse_starlette.sse import EventSourceResponse
from datetime import datetime, timedelta, timezone
from etl import ETLIncremental
import oracledb as cx_Oracle
from pathlib import Path
from dotenv import load_dotenv
from test_arcgis import area_drenaje_geojson,area_sectores_geojson

load_dotenv()

app = FastAPI()

etl = ETLIncremental()

BASE_DIR = Path(__file__).resolve().parent

TZ = ZoneInfo("America/Lima")

ORACLE_DSN = os.getenv("ORACLE_DSN")
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASS = os.getenv("ORACLE_PASS")

ASSETS_DIR = BASE_DIR / "assets"
STATIC_DIR = BASE_DIR / "static"

# Monta /assets → carpeta assets/
app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")

# (opcional) si tienes una SPA o archivos en /static
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

def parse_iso(s: str) -> datetime:
    # Acepta ISO con/ sin tz; normaliza a TZ Lima
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(TZ)

def to_iso(dt: datetime) -> str:
    return dt.astimezone(TZ).isoformat()

def consultar_oracle_rango(dt_start: datetime, dt_end: datetime) -> list[dict]:
    # Como ORIENTAFECINC es VARCHAR2 'YYYY-MM-DD' (sin hora),
    # comparamos por fecha (date) para evitar ORA-01861.
    s = dt_start.astimezone(TZ).date()
    e = dt_end.astimezone(TZ).date()

    conn = cx_Oracle.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=ORACLE_DSN)
    cur = conn.cursor()

    try:
        # (opcional) zona horaria sesión
        try:
            cur.execute("ALTER SESSION SET time_zone = '-05:00'")
        except Exception:
            pass

        sql = """
            SELECT ORIENTAID, ORIENTAFECINC, DEPARTAMENTONOMBRE, PROVINCIANOMBRE, DISTRITONOMBRE,
                   TIPOLOGIA, ORIENTALAT, ORIENTALON
            FROM PRUEBA_CAMI_FILTRO_V2
            WHERE TO_DATE(ORIENTAFECINC, 'YYYY-MM-DD') BETWEEN :p_start AND :p_end
            ORDER BY ORIENTAID ASC
        """

        cur.execute(sql, {"p_start": s, "p_end": e})

        cols = [c[0].lower() for c in cur.description]
        out = []

        for row in cur:
            rec = dict(zip(cols, row))

            # ORIENTAFECINC viene como string 'YYYY-MM-DD'
            f = rec.get("orientafecinc")
            fecha_iso = f"{f}T00:00:00-05:00" if isinstance(f, str) and f else None

            out.append({
                "id": rec.get("orientaid"),
                "fecha": fecha_iso,
                "departamento": rec.get("departamentonombre"),
                "provincia": rec.get("provincianombre"),
                "distrito": rec.get("distritonombre"),
                "tipologia": rec.get("tipologia"),
                "lat": float(rec["orientalat"]) if rec.get("orientalat") is not None else None,
                "lng": float(rec["orientalon"]) if rec.get("orientalon") is not None else None,
            })

        return out

    finally:
        try:
            cur.close()
        finally:
            conn.close()
            
            
# # Función para iniciar el ETL en segundo plano
# @app.on_event("startup")
# async def start_etl():
#     """Inicia el proceso ETL en segundo plano al arrancar la aplicación"""
#     asyncio.create_task(etl.loop_etl(intervalo_segundos=300))  # 5 minutos


def serialize_item(item):
    """Convierte datetime a string ISO para serialización JSON."""
    for key, value in item.items():
        if isinstance(value, datetime):
            item[key] = value.isoformat()
    return item

# Sirve ./static en /static
app.mount("/static", StaticFiles(directory="static"), name="static")

# Servir index.html al root "/"
@app.get("/")
async def root():
    # Si quieres redirigir a /static/index.html:
    # return RedirectResponse(url="/static/index.html", status_code=307)
    # O devolver directamente el archivo:
    return FileResponse("static/index.html")

# ===== SSE =====
event_queue: asyncio.Queue[str] = asyncio.Queue()

@app.get("/events")
async def sse(request: Request):
    async def gen():
        yield "event: connected\ndata: {}\n\n"
        while True:
            if await request.is_disconnected():
                break
            payload = await event_queue.get()
            yield f"data: {payload}\n\n"

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(gen(), media_type="text/event-stream", headers=headers)

# Endpoint de prueba para emitir puntos
@app.post("/emit")
async def emit_point(data: dict = Body(...)):
    # espera {"lat": -12.0464, "lng": -77.0428, "label": "Lima"}
    if "lat" not in data or "lng" not in data:
        return {"ok": False, "error": "lat/lng requeridos"}
    await event_queue.put(json.dumps(data))
    return {"ok": True}


@app.get("/stream")
async def stream(start: str = Query(...), end: str = Query(...)):
    """
    SSE: si el rango incluye hoy (Lima), seguirá emitiendo.
    Si no incluye hoy, emite una vez y finaliza con 'complete'.
    """
    dt_start = parse_iso(start)
    dt_end = parse_iso(end)

    hoy_00 = datetime.now(TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    incluye_hoy = dt_end >= hoy_00

    async def gen():
        # ping inicial
        yield {"event": "ping", "data": "ok"}

        # Estado para dif/ actualización
        last_count = -1
        last_max_id = None

        first_sleep = True
        while True:
            data = consultar_oracle_rango(
                dt_start,
                datetime.now(TZ) if incluye_hoy else dt_end
            )

            # Heurística de cambio: tamaño o max(id)
            count = len(data)
            max_id = data[-1]["id"] if data else None
            changed = (count != last_count) or (max_id != last_max_id)

            if changed:
                last_count = count
                last_max_id = max_id
                payload = json.dumps(data, ensure_ascii=False)
                yield {"event": "update", "data": payload}

            # Si NO incluye hoy → emite una vez y termina
            if not incluye_hoy:
                yield {"event": "complete", "data": "done"}
                break

            # Ritmo de sondeo: 2s la primera, luego 15min
            await asyncio.sleep(2 if first_sleep else 900)
            first_sleep = False

    return EventSourceResponse(
        gen(),
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

@app.get("/query")
def query_once(start: str = Query(...), end: str = Query(...)):
    """
    Consulta puntual sin streaming. Útil cuando el rango NO incluye hoy.
    start/end en ISO (ej: 2025-11-11T00:00:00-05:00).
    """
    dt_start = parse_iso(start)
    dt_end = parse_iso(end)
    data = consultar_oracle_rango(dt_start, dt_end)
    return JSONResponse(content=data)


@app.get("/api/areaDrenaje")
def apiAreasDrenaje():
    try:
        data = area_drenaje_geojson()
        return JSONResponse(content=data)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/api/areaSectores")
def apiSectoresOperacionales():
    try:
        data = area_sectores_geojson()
        return JSONResponse(content=data)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
