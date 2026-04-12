from fastapi import FastAPI, Form, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from app import service
import threading

app = FastAPI()

templates = Jinja2Templates(directory="app/templates")


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request}
    )


@app.post("/run")
def run(
    request: Request,
    source: str = Form(...),
    dest: str = Form(...),
    tables: str = Form(""),
    query: str = Form(""),
    chunk_size: int = Form(1000)
):

    table_list = [t.strip() for t in tables.split(",") if t.strip()]

    thread = threading.Thread(
        target=service.run_copy,
        args=(source, dest, table_list, query, chunk_size),
        daemon=True
    )
    thread.start()

    return {
        "status": "🚀 Rodando em background",
        "source": source,
        "dest": dest,
        "chunk_size": chunk_size,
        "tables": table_list,
        "query": query or "nenhuma"
    }