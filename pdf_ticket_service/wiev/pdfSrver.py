import bottle

from config import settings
from model.service import TicketService

app = bottle.Bottle()
_service = TicketService(output_dir=settings.output_dir)


@app.get('/t/<ticket_id>')
def get_ticket(ticket_id):
    result = _service.get_ticket_by_id(ticket_id)
    if result.success:
        return bottle.static_file(result.filename, root=_service.output_dir, mimetype='application/pdf')
    bottle.abort(404, result.error)
