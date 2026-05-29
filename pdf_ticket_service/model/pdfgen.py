import reportlab
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas


def generate_pdf(data, filename):
    c = canvas.Canvas(filename, pagesize=letter)
    width, height = letter

    c.setFont("Helvetica-Bold", 16)
    c.drawString(100, height - 50, "Generated PDF Report")

    c.setFont("Helvetica", 12)
    text = c.beginText(100, height - 100)
    for key, value in data.items():
        text.textLine(f"{key}: {value}")
    c.drawText(text)

    c.save()
    
