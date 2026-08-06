@echo off
setlocal

set "QM_ROOT=%~dp0"
set "QM_PYTHON=%QM_ROOT%.venv\Scripts\python.exe"

if not exist "%QM_PYTHON%" (
    echo QuantMaster virtual environment was not found: "%QM_ROOT%.venv"
    echo Create it with Python 3.12 or newer, then install the project.
    exit /b 1
)

pushd "%QM_ROOT%" >nul
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%QM_ROOT%tools\start_free_stockdb.ps1"
"%QM_PYTHON%" -m quantmaster.cli serve %*
set "QM_EXIT_CODE=%ERRORLEVEL%"
popd >nul

exit /b %QM_EXIT_CODE%
