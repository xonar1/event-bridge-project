from pydantic import BaseModel, EmailStr
from datetime import datetime, timezone
import uuid


class RegistrationEvent(BaseModel):
    registration_id: str = str(uuid.uuid4())
    event_name: str
    user_email: EmailStr
    user_name: str
    is_vip: bool = False
    registration_time: str = datetime.now(timezone.utc).isoformat()
