# message_contract.py
from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime, timezone
import uuid

class RegistrationEvent(BaseModel):
    registration_id: str = str(uuid.uuid4())
    event_name: str
    user_email: EmailStr
    user_name: str
    is_vip: bool = False
    registration_time: str = datetime.now(timezone.utc).isoformat()

# Пример готового сообщения для тестов
TEST_VIP_EVENT = RegistrationEvent(
    event_name="Python Summit 2026",
    user_email="student@university.ru",
    user_name="Андрей",
    is_vip=True
).model_dump()