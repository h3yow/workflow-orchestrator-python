"""本地入口：uv run uvicorn main:app --reload"""
from app.api import create_app

app = create_app()
