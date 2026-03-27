@echo off
REM Activate virtual environment and run main.py
setlocal enabledelayedexpansion

cd /d "%~dp0"

REM Activate virtual environment
call myvenv\Scripts\activate.bat

REM Run the ingestion pipeline
python main.py

REM Pause so user can see results
pause
