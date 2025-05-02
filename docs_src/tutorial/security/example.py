from esmerald import Security, post
from esmerald.security.http import HTTPBasicCredentials, HTTPBearer

from .tasks import send_welcome

bearer = HTTPBearer()

@post(path="/signup", security=[ bearer])
async def signup(
    payload: SignupPayload,
    creds: HTTPBasicCredentials = Security(bearer)
):
    # Validate token (e.g., introspect JWT)
    if not verify_token(creds.credentials):
        raise HTTPException(status_code=401, detail="Invalid token")
    await send_welcome.enqueue(payload=payload.email)
    return {"status": "queued"}
