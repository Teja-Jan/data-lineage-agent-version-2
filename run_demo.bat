@echo off
:: Change directory to the folder where this batch file is located
cd /d "%~dp0"

echo.
echo =====================================================
echo 🚀 Launching Data Lineage Agent UI...
echo =====================================================
echo.

IF EXIST "venv\Scripts\streamlit.exe" (
    "venv\Scripts\streamlit.exe" run src\app.py
) ELSE (
    echo [ERROR] Virtual environment not found! 
    echo Please ensure you are in the project root:
    echo %cd%
    pause
)
