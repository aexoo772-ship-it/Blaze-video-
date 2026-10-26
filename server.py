"""
Сервер-релей для ESP32 камеры на Render.
ESP32 шлёт кадры сюда, клиенты читают MJPEG-поток.
"""

import asyncio
import os
import time
from aiohttp import web
import aiohttp_cors

# ===========================
# ХРАНИЛИЩЕ ПОСЛЕДНЕГО КАДРА
# ===========================
class FrameBuffer:
    def __init__(self):
        self.frame = None          # bytes JPEG
        self.timestamp = 0         # время получения
        self.event = asyncio.Event()  # сигнал «пришёл новый кадр»
        self.clients = 0
        self.esp_online_at = 0     # время последнего POST от ESP32

    def put(self, data: bytes):
        self.frame = data
        self.timestamp = time.time()
        self.esp_online_at = time.time()
        self.event.set()
        self.event.clear()

    def get(self):
        return self.frame, self.timestamp

    def is_esp_online(self, timeout=15.0) -> bool:
        return (time.time() - self.esp_online_at) < timeout


buf = FrameBuffer()


# ===========================
# ХЕНДЛЕРЫ
# ===========================
async def upload_handler(request: web.Request) -> web.Response:
    """
    ESP32 делает POST сюда с JPEG в теле.
    Content-Type: image/jpeg
    """
    try:
        data = await request.read()
        if not data or len(data) < 100:
            return web.Response(status=400, text="Пустой кадр")

        # Проверяем, что это JPEG (маркер FF D8)
        if data[0] != 0xFF or data[1] != 0xD8:
            return web.Response(status=400, text="Не JPEG")

        buf.put(data)
        return web.Response(text="OK")
    except Exception as e:
        print(f"[upload] Ошибка: {e}")
        return web.Response(status=500, text=str(e))


async def stream_handler(request: web.Request) -> web.StreamResponse:
    """
    Клиент подключается — получает MJPEG-поток из последнего кадра.
    """
    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "multipart/x-mixed-replace; boundary=frame",
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Connection": "close",
            "Access-Control-Allow-Origin": "*",
        },
    )
    await response.prepare(request)
    buf.clients += 1

    try:
        while True:
            # Ждём новый кадр (или таймаут 1 сек, чтобы проверить живость соединения)
            try:
                await asyncio.wait_for(buf.event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

            frame, ts = buf.get()
            if frame is None:
                # ESP32 ещё ничего не прислала — ждём
                continue

            # Если ESP32 молчит >15 сек — закрываем поток
            if not buf.is_esp_online():
                await response.write(b"--frame\r\nContent-Type: text/plain\r\n\r\nESP32 offline\r\n")
                break

            header = (
                b"--frame\r\n"
                b"Content-Type: image/jpeg\r\n"
                b"Content-Length: " + str(len(frame)).encode() + b"\r\n\r\n"
            )
            await response.write(header + frame + b"\r\n")

    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[stream] Ошибка: {e}")
    finally:
        buf.clients -= 1

    return response


async def health_handler(request: web.Request) -> web.Response:
    """JSON-статус: жив ли ESP32, сколько клиентов."""
    return web.json_response({
        "esp32": "online" if buf.is_esp_online() else "offline",
        "last_frame_age_sec": round(time.time() - buf.timestamp, 1) if buf.timestamp else None,
        "clients": buf.clients,
        "has_frame": buf.frame is not None,
    })


async def config_handler(request: web.Request) -> web.Response:
    """
    ESP32 при старте спрашивает у сервера его адрес.
    Возвращает URL, куда слать кадры.
    """
    # Render даёт HTTPS-домен; берём его из заголовка Host
    host = request.headers.get("Host", "")
    scheme = "https" if request.headers.get("X-Forwarded-Proto") == "https" else "http"
    base = f"{scheme}://{host}"
    return web.json_response({
        "upload_url": f"{base}/upload",
        "stream_url": f"{base}/stream",
        "interval_ms": 100,
    })


async def snapshot_handler(request: web.Request) -> web.Response:
    """Один текущий кадр как JPEG."""
    frame, _ = buf.get()
    if frame is None:
        return web.Response(status=503, text="Кадров ещё нет")
    return web.Response(
        body=frame,
        content_type="image/jpeg",
        headers={"Cache-Control": "no-store", "Access-Control-Allow-Origin": "*"},
    )


# ===========================
# ЗАПУСК
# ===========================
def make_app() -> web.Application:
    app = web.Application(client_max_size=1024 * 1024)  # 1 MB лимит на POST
    app.router.add_post("/upload", upload_handler)
    app.router.add_get("/stream", stream_handler)
    app.router.add_get("/snapshot", snapshot_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_get("/config", config_handler)

    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=False,
            expose_headers="*",
            allow_headers="*",
            allow_methods=["GET", "POST", "OPTIONS"],
        )
    })
    for route in list(app.router.routes()):
        cors.add(route)

    return app


if __name__ == "__main__":
    app = make_app()
    port = int(os.environ.get("PORT", 8080))
    print(f"Запуск на порту {port}")
    web.run_app(app, host="0.0.0.0", port=port)