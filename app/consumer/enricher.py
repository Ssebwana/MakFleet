import hashlib
from psycopg2.extras import Json

def payload_hash(payload: dict) -> str:
    return hashlib.sha256(str(sorted(payload.items())).encode()).hexdigest()