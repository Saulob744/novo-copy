from fastapi import FastAPI, Form, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from app import service
import threading

app = FastAPI()
templates = Jinja2Templates(directory="app/templates")

# Variável global para o status (Resolve o Status)
PROGRESSO = {"message": "Aguardando...", "progress": "0%", "running": False, "error": None}

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        request=request, 
        name="index.html"
    )

@app.get("/status")
def get_status():
    return PROGRESSO

@app.post("/run")
def run(
    source: str = Form(...),
    dest: str = Form(...),
    tables: str = Form(""),
    query: str = Form(""),
    chunk_size: int = Form(1000)
):
    global PROGRESSO
    PROGRESSO = {"message": "🚀 Iniciando...", "progress": "0%", "running": True, "error": None}

    table_list = [t.strip() for t in tables.split(",") if t.strip()]

    # Dispara o serviço em segundo plano
    thread = threading.Thread(
        target=service.run_copy,
        args=(source, dest, table_list, query, chunk_size),
        daemon=True
    )
    thread.start()

    return {"status": "🚀 Rodando em background"}