import threading
from wiev.pdfSrver import app
from controller.consumer import run_consumer


def start_server():
    app.run(host='127.0.0.1', port=8080)


def run_all():
    threading.Thread(target=start_server, daemon=True).start()
    threading.Thread(target=run_consumer, daemon=True).start()
    print("Server running on http://127.0.0.1:8080")
    print("RabbitMQ consumer listening for registration events...")

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("Shutting down.")
