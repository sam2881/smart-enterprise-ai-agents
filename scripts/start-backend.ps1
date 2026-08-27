$env:PYTHONPATH = 'd:\projects\ai_agent_app'
Set-Location 'd:\projects\ai_agent_app'
python -m uvicorn backend.app:app --host 0.0.0.0 --port 8000 2>&1 | Out-File 'd:\projects\ai_agent_app\logs\backend.log' -Encoding utf8
