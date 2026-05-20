import logging
import os
from dataclasses import dataclass, field
from typing import Optional
from .eventReger import RegistrationEvent
from .pdfgen import generate_pdf

logger = logging.getLogger(__name__)


@dataclass
class TicketResult:
    success: bool
    filename: Optional[str] = None
    error: Optional[str] = None
    registration_id: Optional[str] = field(default=None)


class TicketService:
    def __init__(self, output_dir: str = "./generated_tickets"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def process_event(self, event: RegistrationEvent) -> TicketResult:
        try:
            self._validate(event)
            return self._generate(event)
        except ValueError as ve:
            logger.warning("Validation failed for %s: %s", event.registration_id, ve)
            return TicketResult(success=False, error=str(ve), registration_id=event.registration_id)
        except Exception as exc:
            logger.error("Unexpected error processing %s: %s", event.registration_id, exc)
            return TicketResult(success=False, error=str(exc), registration_id=event.registration_id)

    def _validate(self, event: RegistrationEvent) -> None:
        if not event.user_email:
            raise ValueError("user_email is required")
        if not event.user_name:
            raise ValueError("user_name is required")
        if not event.event_name:
            raise ValueError("event_name is required")

    def _generate(self, event: RegistrationEvent) -> TicketResult:
        filename = os.path.join(
            self.output_dir,
            f"ticket_{event.registration_id}.pdf"
        )
        data = event.model_dump()
        generate_pdf(data, filename)
        logger.info("Ticket generated: %s", filename)
        return TicketResult(
            success=True,
            filename=filename,
            registration_id=event.registration_id,
        )

    def get_ticket_by_id(self, ticket_id: str) -> TicketResult:
        filename = os.path.join(self.output_dir, f"ticket_{ticket_id}.pdf")
        if os.path.exists(filename):
            return TicketResult(success=True, filename=filename, registration_id=ticket_id)
        return TicketResult(success=False, error=f"ticket_{ticket_id}.pdf not found", registration_id=ticket_id)
