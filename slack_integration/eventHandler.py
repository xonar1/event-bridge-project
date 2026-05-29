from typing import Any, Dict, Optional

from message_contract import RegistrationEvent

VIP_EMOJI = "\U0001f451"
REGULAR_EMOJI = "\U0001f3ab"
CALENDAR_EMOJI = "\U0001f4c5"
USER_EMOJI = "\U0001f464"
EMAIL_EMOJI = "\U0001f4e7"
ROCKET_EMOJI = "\U0001f680"


def format_slack_message(event: RegistrationEvent, ticket_url: Optional[str] = None) -> Dict[str, Any]:
    status_emoji = VIP_EMOJI if event.is_vip else REGULAR_EMOJI
    vip_badge = "👑 **VIP**" if event.is_vip else "—"

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{ROCKET_EMOJI}  Новая регистрация!", "emoji": True},
        },
        {"type": "divider"},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*{USER_EMOJI} Участник:*\n{event.user_name}"},
                {"type": "mrkdwn", "text": f"*{EMAIL_EMOJI} Email:*\n{event.user_email}"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*{CALENDAR_EMOJI} Мероприятие:*\n{event.event_name}"},
                {"type": "mrkdwn", "text": f"*{status_emoji} Статус:*\n{vip_badge}"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*🆔 ID:*\n`{event.registration_id}`"},
                {"type": "mrkdwn", "text": f"*🕒 Время:*\n{event.registration_time}"},
            ],
        },
    ]

    if ticket_url:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*<{ticket_url}|🎟️ Скачать билет>*"},
        })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": f"{ROCKET_EMOJI} EventBridge 2.0 — Slack Integration"}],
    })

    return {"text": _plain_text_fallback(event, ticket_url), "blocks": blocks}


def _plain_text_fallback(event: RegistrationEvent, ticket_url: Optional[str] = None) -> str:
    vip_suffix = " 👑 VIP" if event.is_vip else ""
    ticket_suffix = f" | Билет: {ticket_url}" if ticket_url else ""
    return (
        f"Новая регистрация: {event.user_name} ({event.user_email})"
        f" на мероприятие {event.event_name}.{vip_suffix}{ticket_suffix}"
    )
