# Don't Remove Credit @VJ_Bots
# Subscribe YouTube Channel For Amazing Bot @Tech_VJ
# Ask Doubt on telegram @KingVJ01

import re, math, logging, secrets, mimetypes, time, json
from info import *
from aiohttp import web
from aiohttp.http_exceptions import BadStatusLine
from TechVJ.bot import multi_clients, work_loads, TechVJBot
from TechVJ.server.exceptions import FIleNotFound, InvalidHash
from TechVJ import StartTime, __version__
from TechVJ.util.custom_dl import ByteStreamer
from TechVJ.util.time_format import get_readable_time
from TechVJ.util.render_template import render_page
from TechVJ.util.link_utils import validate_link
from TechVJ.util.bucket_storage import bucket_enabled, get_cached_presigned_url, migrate_in_background
from database.connections_mdb import increment_video_download

routes = web.RouteTableDef()


def _link_is_valid(request, message_id, secure_hash, quality=""):
    expires = request.rel_url.query.get("exp")
    signature = request.rel_url.query.get("sig")
    return validate_link(message_id, secure_hash, expires, signature, quality)


def _expired_response():
    return web.Response(
        status=410,
        text="This link has expired. Please generate a new link.",
        content_type="text/plain",
    )


@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.json_response("BenFilterBot")


@routes.get(r"/watch/{path:\S+}", allow_head=True)
async def stream_handler(request: web.Request):
    try:
        path = request.match_info["path"]
        match = re.search(r"^([a-zA-Z0-9_-]{6})(\d+)$", path)
        if match:
            secure_hash = match.group(1)
            id = int(match.group(2))
        else:
            id = int(re.search(r"(\d+)(?:/\S+)?", path).group(1))
            secure_hash = request.rel_url.query.get("hash")

        quality = request.rel_url.query.get("quality", "").lower()
        if not _link_is_valid(request, id, secure_hash, quality):
            return _expired_response()

        return web.Response(
            text=await render_page(
                id,
                secure_hash,
                request.rel_url.query.get("exp"),
                request.rel_url.query.get("sig"),
                quality,
            ),
            content_type="text/html",
        )
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except (AttributeError, BadStatusLine, ConnectionResetError):
        pass
    except Exception as e:
        logging.critical(e.with_traceback(None))
        raise web.HTTPInternalServerError(text=str(e))


def _preparing_response(file_id: int, retry_seconds: int = 4):
    # Tiny HTML page only - no video bytes touch Railway. Auto-refreshes
    # until the background migration finishes and the bucket redirect
    # becomes available.
    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="{retry_seconds}">
<title>Preparing your video...</title>
<style>
body {{ font-family: sans-serif; background:#0f0f0f; color:#eee; display:flex;
        align-items:center; justify-content:center; height:100vh; margin:0; }}
.box {{ text-align:center; }}
.spinner {{ width:36px; height:36px; margin:0 auto 16px; border:4px solid #333;
            border-top-color:#4da3ff; border-radius:50%; animation:spin 1s linear infinite; }}
@keyframes spin {{ to {{ transform: rotate(360deg); }} }}
</style>
</head>
<body>
<div class="box">
<div class="spinner"></div>
<p>Preparing your video for streaming...<br>This page will refresh automatically.</p>
</div>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html", status=202)


@routes.get(r"/media/{path:\S+}", allow_head=True)
async def bucket_media_handler(request: web.Request):
    """Redirect to the Railway Bucket once the file is cached there.

    Video bytes never pass through Railway - the bot pre-warms the bucket
    cache as soon as a link is generated (see link_utils.make_stream_links),
    so this route usually just redirects immediately. If the migration
    hasn't finished yet (e.g. link opened right away, or a very large file),
    this shows a small "preparing" page that auto-refreshes rather than
    streaming the video through Railway or hanging the connection.
    """
    try:
        path = request.match_info["path"].lstrip("/")
        match = re.search(r"^([a-zA-Z0-9_-]{6})(\d+)(?:/.*)?$", path)
        if match:
            secure_hash = match.group(1)
            file_id = int(match.group(2))
        else:
            match_id = re.search(r"(\d+)(?:/\S+)?", path)
            if not match_id:
                raise web.HTTPBadRequest(text="Invalid media path")
            file_id = int(match_id.group(1))
            secure_hash = request.rel_url.query.get("hash")

        quality = request.rel_url.query.get("quality", "").lower()
        expires = request.rel_url.query.get("exp")
        signature = request.rel_url.query.get("sig")
        if not validate_link(file_id, secure_hash, expires, signature, quality):
            return _expired_response()

        if not bucket_enabled():
            # Fail closed: never stream video bytes through Railway.
            raise web.HTTPServiceUnavailable(
                text="Video delivery storage is temporarily unavailable. Please try again later."
            )

        started = time.monotonic()
        cached_target = await get_cached_presigned_url(file_id, int(expires))
        if cached_target:
            logging.info(
                "Bucket media ready: file_id=%s elapsed=%.2fs",
                file_id,
                time.monotonic() - started,
            )
            raise web.HTTPFound(location=cached_target)

        # Not cached yet - make sure a migration is actually running (covers
        # the case this link was generated before the pre-warm code existed,
        # or the background task died) and show a lightweight waiting page
        # instead of hanging the request or falling back to Railway streaming.
        migrate_in_background(file_id)
        return _preparing_response(file_id)
    except web.HTTPException:
        raise
    except InvalidHash as e:
        raise web.HTTPForbidden(text=e.message)
    except FIleNotFound as e:
        raise web.HTTPNotFound(text=e.message)
    except Exception as e:
        logging.exception("Bucket media delivery failed")
        raise web.HTTPServiceUnavailable(text="Unable to prepare this video for delivery.")


@routes.get(r"/{path:\S+}", allow_head=True)
async def legacy_media_redirect_handler(request: web.Request):
    """Preserve older media links without streaming bytes through Railway."""
    try:
        path = request.match_info["path"].lstrip("/")
        match = re.search(r"^([a-zA-Z0-9_-]{6})(\d+)(?:/.*)?$", path)
        if match:
            secure_hash = match.group(1)
            file_id = int(match.group(2))
        else:
            match_id = re.search(r"(\d+)(?:/\S+)?", path)
            if not match_id:
                raise web.HTTPNotFound(text="File not found")
            file_id = int(match_id.group(1))
            secure_hash = request.rel_url.query.get("hash")

        quality = request.rel_url.query.get("quality", "").lower()
        if not validate_link(
            file_id,
            secure_hash,
            request.rel_url.query.get("exp"),
            request.rel_url.query.get("sig"),
            quality,
        ):
            return _expired_response()

        # Normalize accidental leading slashes so old links never become
        # /media//664/... or /media////664/....
        target = request.rel_url.with_path("/media/" + path.lstrip("/"))
        raise web.HTTPFound(location=str(target))
    except web.HTTPException:
        raise
    except Exception as e:
        logging.exception("Legacy media redirect failed")
        raise web.HTTPInternalServerError(text="Unable to redirect this media link.")


# In-memory active viewer registry: {video_id: {viewer_id: last_seen_monotonic}}
_ACTIVE_VIEWERS = {}
_VIEWER_TTL = 35

def _viewer_count(video_id):
    now = time.monotonic()
    viewers = _ACTIVE_VIEWERS.setdefault(int(video_id), {})
    stale = [k for k, v in viewers.items() if now - v > _VIEWER_TTL]
    for k in stale:
        viewers.pop(k, None)
    if not viewers:
        _ACTIVE_VIEWERS.pop(int(video_id), None)
        return 0
    return len(viewers)


@routes.get(r"/api/viewers/{file_id}")
async def viewer_count_handler(request: web.Request):
    try:
        file_id = int(request.match_info["file_id"])
        return web.json_response({"viewers": _viewer_count(file_id)})
    except (TypeError, ValueError):
        raise web.HTTPBadRequest(text="Invalid file ID")


@routes.post(r"/api/viewers/{file_id}")
async def viewer_heartbeat_handler(request: web.Request):
    try:
        file_id = int(request.match_info["file_id"])
        data = await request.json()
        viewer_id = str(data.get("viewer_id", "")).strip()
        active = bool(data.get("active", False))
        if not viewer_id or len(viewer_id) > 128:
            raise web.HTTPBadRequest(text="Invalid viewer ID")
        viewers = _ACTIVE_VIEWERS.setdefault(file_id, {})
        if active:
            viewers[viewer_id] = time.monotonic()
        else:
            viewers.pop(viewer_id, None)
        return web.json_response({"viewers": _viewer_count(file_id)})
    except json.JSONDecodeError:
        raise web.HTTPBadRequest(text="Invalid JSON")
    except (TypeError, ValueError):
        raise web.HTTPBadRequest(text="Invalid file ID")


@routes.post(r"/api/download/{file_id}")
async def download_count_handler(request: web.Request):
    try:
        file_id = int(request.match_info["file_id"])
        count = await increment_video_download(file_id)
        return web.json_response({"downloads": count})
    except (TypeError, ValueError):
        raise web.HTTPBadRequest(text="Invalid file ID")


class_cache = {}


async def media_streamer(request: web.Request, id: int, secure_hash: str):
    range_header = request.headers.get("Range", 0)

    index = min(work_loads, key=work_loads.get)
    faster_client = multi_clients[index]

    if MULTI_CLIENT:
        logging.info(f"Client {index} is now serving {request.remote}")

    if faster_client in class_cache:
        tg_connect = class_cache[faster_client]
        logging.debug(f"Using cached ByteStreamer object for client {index}")
    else:
        logging.debug(f"Creating new ByteStreamer object for client {index}")
        tg_connect = ByteStreamer(faster_client)
        class_cache[faster_client] = tg_connect
    logging.debug("before calling get_file_properties")
    file_id = await tg_connect.get_file_properties(id)
    logging.debug("after calling get_file_properties")

    if file_id.unique_id[:6] != secure_hash:
        logging.debug(f"Invalid hash for message with ID {id}")
        raise InvalidHash

    file_size = file_id.file_size

    if range_header:
        from_bytes, until_bytes = range_header.replace("bytes=", "").split("-")
        from_bytes = int(from_bytes)
        until_bytes = int(until_bytes) if until_bytes else file_size - 1
    else:
        from_bytes = request.http_range.start or 0
        until_bytes = (request.http_range.stop or file_size) - 1

    if (until_bytes > file_size) or (from_bytes < 0) or (until_bytes < from_bytes):
        return web.Response(
            status=416,
            body="416: Range not satisfiable",
            headers={"Content-Range": f"bytes */{file_size}"},
        )

    chunk_size = 1024 * 1024
    until_bytes = min(until_bytes, file_size - 1)

    offset = from_bytes - (from_bytes % chunk_size)
    first_part_cut = from_bytes - offset
    last_part_cut = until_bytes % chunk_size + 1

    req_length = until_bytes - from_bytes + 1
    part_count = math.ceil(until_bytes / chunk_size) - math.floor(offset / chunk_size)
    body = tg_connect.yield_file(
        file_id, index, offset, first_part_cut, last_part_cut, part_count, chunk_size
    )

    mime_type = file_id.mime_type
    file_name = file_id.file_name
    disposition = "attachment"

    if mime_type:
        if not file_name:
            try:
                file_name = f"{secrets.token_hex(2)}.{mime_type.split('/')[1]}"
            except (IndexError, AttributeError):
                file_name = f"{secrets.token_hex(2)}.unknown"
    else:
        if file_name:
            mime_type = mimetypes.guess_type(file_id.file_name)
        else:
            mime_type = "application/octet-stream"
            file_name = f"{secrets.token_hex(2)}.unknown"

    return web.Response(
        status=206 if range_header else 200,
        body=body,
        headers={
            "Content-Type": f"{mime_type}",
            "Content-Range": f"bytes {from_bytes}-{until_bytes}/{file_size}",
            "Content-Length": str(req_length),
            "Content-Disposition": f'{disposition}; filename="{file_name}"',
            "Accept-Ranges": "bytes",
        },
    )
