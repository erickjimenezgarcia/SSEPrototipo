# app.py
import asyncio, json
from fastapi import FastAPI, Body, Request
from fastapi.responses import StreamingResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

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
