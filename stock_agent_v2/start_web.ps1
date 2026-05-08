$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Start-Process powershell -ArgumentList '-NoExit','-Command',"Set-Location '$root'; .\venv_kis\Scripts\python run_web.py"
Start-Process powershell -ArgumentList '-NoExit','-Command','ngrok http --url=unbiased-tight-jay.ngrok-free.app 8000'
