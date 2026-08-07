@echo off
setlocal

set "QM_ROOT=%~dp0"
set "QM_PYTHON=%QM_ROOT%.venv\Scripts\python.exe"
set "QM_LAUNCHER=%QM_ROOT%.venv\Scripts\QuantMaster.exe"

if not exist "%QM_PYTHON%" (
    echo QuantMaster virtual environment was not found: "%QM_ROOT%.venv"
    echo Create it with Python 3.12 or newer, then install the project.
    exit /b 1
)

pushd "%QM_ROOT%" >nul
title QuantMaster
"%QM_PYTHON%" tools\windows_launcher.py --source "%QM_PYTHON%" --icon "packaging\quantmaster.ico" --output "%QM_LAUNCHER%" >nul 2>nul
if not exist "%QM_LAUNCHER%" (
    echo QuantMaster named launcher could not be prepared; falling back to Python.
    set "QM_LAUNCHER=%QM_PYTHON%"
)
rem The repository launcher enables Web hot reload by default.  The reload
rem supervisor owns free-stockdb, so replacing the Web worker never restarts it.
rem Pass --no-reload for the traditional single-process mode.
"%QM_LAUNCHER%" -m quantmaster.cli serve --reload %*
set "QM_EXIT_CODE=%ERRORLEVEL%"
popd >nul

exit /b %QM_EXIT_CODE%
