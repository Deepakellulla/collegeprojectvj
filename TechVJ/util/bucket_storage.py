import asyncio
import logging
import math
import os
import time
from pathlib import Path
from typing import Optional

import boto3
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

from info import DATABASE_NAME, OTHER_DB_URI, LOG_CHANNEL
from TechVJ.bot import TechVJBot, multi_clients, work_loads
from TechVJ.util.file_properties import get_file_ids

logger = logging.getLogger(__name__)

BUCKET_NAME = os.environ.get("BUCKET", "").strip()
BUCKET_ENDPOINT = os.environ.get("ENDPOINT", "").strip()
BUCKET_ACCESS_KEY = os.environ.get("ACCESS_KEY_ID", "").strip()
BUCKET_SECRET_KEY = os.environ.get("SECRET_ACCESS_KEY", "").strip()
BUCKET_REGION = os.environ.get("REGION", "auto").strip() or "auto"

# Safety guard: by default, no more than 250 GB/month is uploaded from the
# Railway service into the bucket. At $0.05/GB service egress, this is $5.00
# of upload egress, leaving room for compute and other traffic under a $20 cap.
UPLOAD_LIMIT_GB = float(os.environ.get("BUCKET_MONTHLY_UPLOAD_GB_LIMIT", "100"))
UPLOAD_LIMIT_BYTES = int(UPLOAD_LIMIT_GB * 1024 * 1024 * 1024)

try:
    import pymongo
    _mongo = pymongo.MongoClient(OTHER_DB_URI)
    _db = _mongo[DATABASE_NAME]
    _cache_col = _db["RAILWAY_BUCKET_CACHE"]
    _budget_col = _db["RAILWAY_BUCKET_BUDGET"]
except Exception:
    _mongo = None
    _cache_col = None
    _budget_col = None

_locks = {}
_locks_guard = asyncio.Lock()
_cleanup_lock = asyncio.Lock()
_last_cleanup = 0.0

# Long-running media operations need explicit network/application timeouts.
# These are configurable because upload speed varies by Railway region/bucket.
TELEGRAM_DOWNLOAD_TIMEOUT = int(
    os.environ.get("BUCKET_TELEGRAM_DOWNLOAD_TIMEOUT", "900")
)
BUCKET_UPLOAD_TIMEOUT = int(
    os.environ.get("BUCKET_UPLOAD_TIMEOUT", "900")
)

# How many bot clients can split a single file's download across. Only
# helps if you've actually set up extra bot tokens (MULTI_TOKEN1, etc.) -
# with just one client this is a no-op and falls back to single-client
# streaming, same as before.
MIGRATION_MAX_PARALLEL_CLIENTS = max(1, int(os.environ.get("BUCKET_MIGRATION_PARALLEL_CLIENTS", "4")))

# Multipart uploads are substantially more reliable for large video files.
TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=8 * 1024 * 1024,
    multipart_chunksize=16 * 1024 * 1024,
    max_concurrency=4,
    use_threads=True,
)


def bucket_enabled() -> bool:
    return bool(BUCKET_NAME and BUCKET_ENDPOINT and BUCKET_ACCESS_KEY and BUCKET_SECRET_KEY)


def _client():
    if not bucket_enabled():
        raise RuntimeError(
            "Railway Storage Bucket is not configured. Set BUCKET, ENDPOINT, "
            "ACCESS_KEY_ID and SECRET_ACCESS_KEY from the Railway Bucket variables."
        )
    return boto3.client(
        "s3",
        endpoint_url=BUCKET_ENDPOINT,
        aws_access_key_id=BUCKET_ACCESS_KEY,
        aws_secret_access_key=BUCKET_SECRET_KEY,
        region_name=BUCKET_REGION,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
            connect_timeout=30,
            read_timeout=120,
            tcp_keepalive=True,
        ),
    )


def _object_key(unique_id: str, file_name: str) -> str:
    # Telegram file_unique_id is stable for the same underlying media.
    safe_name = Path(file_name or "video.bin").name.replace("/", "_")
    return f"media/{unique_id}/{safe_name}"


def _head_object(key: str) -> bool:
    try:
        _client().head_object(Bucket=BUCKET_NAME, Key=key)
        return True
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def _presign(key: str, expires: int) -> str:
    # The signed URL lifetime is bounded by the existing 24h link expiry.
    ttl = max(60, int(expires - time.time()))
    ttl = min(ttl, 7 * 24 * 3600)
    return _client().generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET_NAME, "Key": key},
        ExpiresIn=ttl,
    )


def _period_key() -> str:
    return time.strftime("%Y-%m", time.gmtime())


def _reserve_upload(size: int) -> bool:
    if _budget_col is None:
        # If Mongo is unavailable, fail closed rather than risking unbounded
        # Railway egress.
        return False

    period = _period_key()
    # Single-process lock prevents races in the normal Railway deployment.
    doc = _budget_col.find_one({"_id": period}) or {"bytes": 0}
    used = int(doc.get("bytes", 0))
    if used + size > UPLOAD_LIMIT_BYTES:
        return False
    _budget_col.update_one({"_id": period}, {"$set": {"bytes": used + size}}, upsert=True)
    return True


def _release_upload(size: int):
    if _budget_col is not None:
        _budget_col.update_one({"_id": _period_key()}, {"$inc": {"bytes": -int(size)}})


async def _lock_for(unique_id: str):
    async with _locks_guard:
        lock = _locks.get(unique_id)
        if lock is None:
            lock = asyncio.Lock()
            _locks[unique_id] = lock
        return lock


def _cleanup_old_objects(max_age_seconds: int = 7 * 24 * 3600):
    if _cache_col is None or not bucket_enabled():
        return
    cutoff = time.time() - max_age_seconds
    old = list(_cache_col.find({"updated_at": {"$lt": cutoff}}, {"_id": 1, "key": 1}))
    if not old:
        return
    client = _client()
    for doc in old:
        key = doc.get("key")
        if not key:
            continue
        try:
            client.delete_object(Bucket=BUCKET_NAME, Key=key)
        except Exception:
            logger.exception("Failed to remove old bucket object %s", key)
            continue
        _cache_col.delete_one({"_id": doc.get("_id")})


async def maybe_cleanup_old_objects():
    global _last_cleanup
    if not bucket_enabled():
        return
    now = time.time()
    if now - _last_cleanup < 6 * 3600:
        return
    async with _cleanup_lock:
        now = time.time()
        if now - _last_cleanup < 6 * 3600:
            return
        try:
            await asyncio.to_thread(_cleanup_old_objects)
            _last_cleanup = now
        except Exception:
            logger.exception("Bucket cleanup failed")


def _create_multipart(key: str, mime_type: str, file_name: str) -> str:
    resp = _client().create_multipart_upload(
        Bucket=BUCKET_NAME,
        Key=key,
        ContentType=mime_type or "application/octet-stream",
        ContentDisposition=f'inline; filename="{Path(file_name).name}"',
        CacheControl="private, max-age=0",
    )
    return resp["UploadId"]


def _upload_part(key: str, upload_id: str, part_number: int, data: bytes) -> dict:
    resp = _client().upload_part(
        Bucket=BUCKET_NAME, Key=key, PartNumber=part_number, UploadId=upload_id, Body=data,
    )
    return {"ETag": resp["ETag"], "PartNumber": part_number}


def _complete_multipart(key: str, upload_id: str, parts: list):
    _client().complete_multipart_upload(
        Bucket=BUCKET_NAME, Key=key, UploadId=upload_id,
        MultipartUpload={"Parts": sorted(parts, key=lambda p: p["PartNumber"])},
    )


def _abort_multipart(key: str, upload_id: str):
    try:
        _client().abort_multipart_upload(Bucket=BUCKET_NAME, Key=key, UploadId=upload_id)
    except Exception:
        logger.exception("Failed to abort multipart upload for %s", key)


def _select_migration_clients():
    """Pick up to MIGRATION_MAX_PARALLEL_CLIENTS bot clients to split a
    single file's download across, preferring whichever are least busy
    serving other requests right now (same work_loads counters the viewer
    streamer uses). Falls back to just TechVJBot if no extra clients were
    configured.
    """
    if not multi_clients:
        return [TechVJBot]
    ordered = sorted(multi_clients.items(), key=lambda kv: work_loads.get(kv[0], 0))
    chosen = [client for _, client in ordered[:MIGRATION_MAX_PARALLEL_CLIENTS]]
    return chosen or [TechVJBot]


async def _pipe_telegram_to_bucket(message, key: str, mime_type: str, file_name: str, size: int) -> int:
    """Streams Telegram media straight into a bucket multipart upload,
    splitting the download across multiple bot clients in parallel when
    more than one is available (see MIGRATION_MAX_PARALLEL_CLIENTS).

    The file is divided into contiguous, part-aligned slices - one per
    worker. Each worker downloads its own slice from Telegram (via its own
    client) and uploads each completed 16 MiB part to the bucket as soon as
    it's ready, so workers make progress independently instead of taking
    turns, and within each worker download and upload still overlap rather
    than running sequentially. With N clients available, wall-clock time
    for the Telegram side drops roughly N-fold on top of that overlap.
    With only one client configured, this behaves the same as the
    single-worker streaming path did before.
    """
    part_size = TRANSFER_CONFIG.multipart_chunksize  # 16 MiB
    chunk_size = 1024 * 1024  # stream_media's fixed chunk size
    part_chunks = part_size // chunk_size

    upload_id = await asyncio.to_thread(_create_multipart, key, mime_type, file_name)

    total_chunks = math.ceil(size / chunk_size)
    total_parts = math.ceil(total_chunks / part_chunks)

    clients = _select_migration_clients()
    num_workers = max(1, min(len(clients), total_parts))

    # Split parts into contiguous, roughly-equal ranges across workers.
    base, extra = divmod(total_parts, num_workers)
    ranges = []
    start_part = 0
    for w in range(num_workers):
        count = base + (1 if w < extra else 0)
        if count > 0:
            ranges.append((start_part, start_part + count))
            start_part += count

    parts: list = []
    total_size_holder = [0]

    async def run_worker(client, part_start: int, part_end: int):
        msg = await client.get_messages(message.chat.id, message.id)
        chunk_offset = part_start * part_chunks
        chunk_count = min(part_end * part_chunks, total_chunks) - chunk_offset

        local_queue: "asyncio.Queue" = asyncio.Queue(maxsize=2)

        async def producer():
            buffer = bytearray()
            part_number = part_start + 1
            async for chunk in client.stream_media(msg, limit=chunk_count, offset=chunk_offset):
                buffer += chunk
                total_size_holder[0] += len(chunk)
                while len(buffer) >= part_size:
                    data = bytes(buffer[:part_size])
                    del buffer[:part_size]
                    await local_queue.put((part_number, data))
                    part_number += 1
            if buffer:
                await local_queue.put((part_number, bytes(buffer)))
            await local_queue.put(None)

        async def consumer():
            while True:
                item = await local_queue.get()
                if item is None:
                    return
                part_number, data = item
                uploaded = await asyncio.to_thread(_upload_part, key, upload_id, part_number, data)
                parts.append(uploaded)

        await asyncio.gather(producer(), consumer())

    results = await asyncio.gather(
        *[run_worker(clients[i % len(clients)], p_start, p_end) for i, (p_start, p_end) in enumerate(ranges)],
        return_exceptions=True,
    )
    error = next((r for r in results if isinstance(r, Exception)), None)

    if error:
        await asyncio.to_thread(_abort_multipart, key, upload_id)
        raise error

    if not parts:
        await asyncio.to_thread(_abort_multipart, key, upload_id)
        raise RuntimeError("Telegram media stream produced no data.")

    await asyncio.to_thread(_complete_multipart, key, upload_id, parts)
    return total_size_holder[0]


async def ensure_uploaded(file_id: int):
    """Ensure a Telegram media file exists in Railway Bucket.

    Returns (object_key, file_data). The first request downloads the file once
    to Railway ephemeral disk, uploads it to the bucket, then removes the temp
    file. Later requests never stream the file through Railway.
    """
    if not bucket_enabled():
        raise RuntimeError("Railway Storage Bucket is not configured.")

    await maybe_cleanup_old_objects()
    file_data = await get_file_ids(TechVJBot, int(LOG_CHANNEL), int(file_id))
    unique_id = str(file_data.unique_id)
    file_name = file_data.file_name or f"{unique_id}.bin"
    key = _object_key(unique_id, file_name)

    # Fast path: Mongo cache, then S3 HEAD as a repair path.
    if _cache_col is not None:
        cached = _cache_col.find_one({"_id": unique_id}, {"key": 1})
        if cached and cached.get("key") == key:
            try:
                exists = await asyncio.to_thread(_head_object, key)
                if exists:
                    return key, file_data
            except Exception:
                logger.exception("Bucket cache verification failed for %s", unique_id)

    lock = await _lock_for(unique_id)
    async with lock:
        if await asyncio.to_thread(_head_object, key):
            if _cache_col is not None:
                _cache_col.update_one(
                    {"_id": unique_id},
                    {"$set": {"key": key, "size": int(file_data.file_size or 0), "updated_at": time.time()}},
                    upsert=True,
                )
            return key, file_data

        size = int(file_data.file_size or 0)
        if size <= 0:
            raise RuntimeError("Telegram did not provide a valid file size.")

        # Hard monthly upload budget. We intentionally fail closed instead of
        # falling back to Telegram->Railway->viewer streaming.
        if not await asyncio.to_thread(_reserve_upload, size):
            raise RuntimeError(
                f"Monthly media migration safety limit reached ({UPLOAD_LIMIT_GB:g} GB). "
                "Video delivery is paused to protect the Railway budget."
            )

        reserved = True
        try:
            logger.info("Migrating Telegram media %s (%s bytes) to Railway Bucket", unique_id, size)
            message = await TechVJBot.get_messages(int(LOG_CHANNEL), int(file_id))
            if message.empty:
                raise RuntimeError("Source Telegram message was not found.")

            # Stream Telegram -> bucket concurrently instead of downloading
            # the whole file to disk first. Railway ingress from Telegram is
            # not billed as network egress; only the bucket upload counts.
            logger.info(
                "Starting piped Telegram->bucket transfer: file=%s key=%s size=%s timeout=%ss",
                unique_id,
                key,
                size,
                BUCKET_UPLOAD_TIMEOUT,
            )
            try:
                actual_size = await asyncio.wait_for(
                    _pipe_telegram_to_bucket(message, key, file_data.mime_type, file_name, size),
                    timeout=max(TELEGRAM_DOWNLOAD_TIMEOUT, BUCKET_UPLOAD_TIMEOUT),
                )
            except asyncio.TimeoutError:
                raise RuntimeError(
                    f"Media transfer timed out after "
                    f"{max(TELEGRAM_DOWNLOAD_TIMEOUT, BUCKET_UPLOAD_TIMEOUT)}s."
                )

            if actual_size != size:
                logger.warning("Telegram size mismatch: metadata=%s actual=%s", size, actual_size)

            # Verify the object before telling the browser that it is ready.
            logger.info("Verifying bucket object: %s", key)
            if not await asyncio.to_thread(_head_object, key):
                raise RuntimeError(
                    f"Bucket upload completed but object was not found: {key}"
                )

            logger.info("Bucket upload completed successfully: %s", key)

            if _cache_col is not None:
                _cache_col.update_one(
                    {"_id": unique_id},
                    {"$set": {
                        "key": key,
                        "size": size,
                        "file_name": file_name,
                        "mime_type": file_data.mime_type or "application/octet-stream",
                        "updated_at": time.time(),
                    }},
                    upsert=True,
                )
            reserved = False
            return key, file_data
        except Exception:
            logger.exception("Media migration failed: file=%s key=%s", unique_id, key)
            if reserved:
                await asyncio.to_thread(_release_upload, size)
            raise


async def get_presigned_url(file_id: int, expires: int) -> str:
    key, _ = await ensure_uploaded(file_id)
    return await asyncio.to_thread(_presign, key, int(expires))


async def get_cached_presigned_url(file_id: int, expires: int) -> Optional[str]:
    """Return a presigned bucket URL only if the object is already migrated.

    Never triggers a Telegram download or bucket upload — this is the fast
    path used to decide whether we can redirect immediately (cheap) or need
    to show the "preparing" page instead of making the viewer wait on a
    blocked connection.
    """
    if not bucket_enabled() or _cache_col is None:
        return None

    file_data = await get_file_ids(TechVJBot, int(LOG_CHANNEL), int(file_id))
    unique_id = str(file_data.unique_id)
    file_name = file_data.file_name or f"{unique_id}.bin"
    key = _object_key(unique_id, file_name)

    cached = _cache_col.find_one({"_id": unique_id}, {"key": 1})
    if not cached or cached.get("key") != key:
        return None

    try:
        exists = await asyncio.to_thread(_head_object, key)
    except Exception:
        logger.exception("Bucket cache verification failed for %s", unique_id)
        return None

    if not exists:
        return None
    return await asyncio.to_thread(_presign, key, int(expires))


def migrate_in_background(file_id: int):
    """Fire-and-forget Telegram -> bucket migration.

    Used to pre-warm the cache (when a link is generated) or to make sure a
    migration is running when a viewer hits the "preparing" page.
    """
    async def _run():
        try:
            await ensure_uploaded(file_id)
        except Exception:
            logger.exception("Background bucket migration failed for file_id=%s", file_id)

    asyncio.create_task(_run())
