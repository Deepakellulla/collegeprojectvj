import hashlib
import hmac
import logging
import os
import time
from urllib.parse import urlencode

from info import BOT_TOKEN, URL

logger = logging.getLogger(__name__)

LINK_EXPIRE_HOURS = float(os.environ.get("LINK_EXPIRE_HOURS", "24"))
LINK_EXPIRE_SECONDS = max(60, int(LINK_EXPIRE_HOURS * 3600))
LINK_SIGNING_SECRET = os.environ.get("LINK_SIGNING_SECRET", BOT_TOKEN)


def _signature(message_id: int, secure_hash: str, expires: int, quality: str = "") -> str:
    payload = f"{int(message_id)}:{secure_hash}:{int(expires)}:{quality.lower()}".encode()
    return hmac.new(LINK_SIGNING_SECRET.encode(), payload, hashlib.sha256).hexdigest()[:32]


def make_link(path: str, message_id: int, file_name: str, secure_hash: str, expires: int | None = None, quality: str = "") -> str:
    if expires is None:
        expires = int(time.time()) + LINK_EXPIRE_SECONDS
    sig = _signature(message_id, secure_hash, expires, quality)
    query = urlencode({"hash": secure_hash, "exp": expires, "sig": sig, **({"quality": quality} if quality else {})})
    return f"{URL}{path}/{int(message_id)}/{file_name}?{query}"


def make_stream_links(message_id: int, file_name: str, secure_hash: str):
    expires = int(time.time()) + LINK_EXPIRE_SECONDS
    stream = make_link("watch", message_id, file_name, secure_hash, expires)
    download = make_link("", message_id, file_name, secure_hash, expires)

    # Start migrating this file into the Railway Bucket the moment the link
    # is handed out, not when the viewer clicks it. By the time they open
    # the watch page, the file is often already cached (or well on its way),
    # so the page's auto-refresh only has to wait out the remainder.
    try:
        from TechVJ.util.bucket_storage import bucket_enabled, migrate_in_background
        if bucket_enabled():
            migrate_in_background(message_id)
    except Exception:
        logger.exception("Failed to pre-warm bucket cache for message_id=%s", message_id)

    return stream, download


def validate_link(message_id: int, secure_hash: str, expires: str | int | None, signature: str | None, quality: str = "") -> bool:
    if not expires or not signature:
        return False
    try:
        expires = int(expires)
    except (TypeError, ValueError):
        return False
    if expires < int(time.time()):
        return False
    expected = _signature(message_id, secure_hash, expires, quality)
    return hmac.compare_digest(str(signature), expected)


def expiry_message(expires: str | int | None = None) -> str:
    if expires:
        try:
            remaining = max(0, int(expires) - int(time.time()))
            hours, rem = divmod(remaining, 3600)
            minutes = rem // 60
            return f"This link expires in {hours}h {minutes:02d}m."
        except (TypeError, ValueError):
            pass
    return "This link has expired."
