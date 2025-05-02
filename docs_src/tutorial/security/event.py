import hashlib
import hmac

from asyncmq.core.event import event_emitter

SECRET = b'supersecret'

def sign_event(e):
    payload = repr(e)
    sig = hmac.new(SECRET, payload.encode(), hashlib.sha256).hexdigest()
    store.append({"event": e, "sig": sig})

event_emitter.on("job:completed", sign_event)
