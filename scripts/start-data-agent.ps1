$env:PYTHONPATH = 'd:\projects\ai_agent_app'
Set-Location 'd:\projects\ai_agent_app\agents\data_agent'
python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8001 2>&1 | Out-File 'd:\projects\ai_agent_app\logs\data-agent.log' -Encoding utf8
