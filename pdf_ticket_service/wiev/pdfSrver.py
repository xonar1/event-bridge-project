import bottle
from model.eventReger import RegistrationEvent
from model.service import TicketService

app = bottle.Bottle()


@app.get('/t/<ticket_id>')
def get_ticket(ticket_id):
    service = TicketService()
    result = service.get_ticket_by_id(ticket_id)
    if result.success:
        return bottle.static_file(result.filename, root=service.output_dir, mimetype='application/pdf')
    else:
        bottle.abort(404, result.error)
