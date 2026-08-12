@echo off
setlocal

for %%I in ("%~dp0..\..") do set "QM_ROOT=%%~fI\"
set "QM_PYTHON=%QM_ROOT%.venv\Scripts\python.exe"
set "QM_LAUNCHER=%QM_ROOT%.venv\Scripts\QuantMaster.exe"

if not exist "%QM_PYTHON%" (
    echo QuantMaster virtual environment was not found: "%QM_ROOT%.venv"
    echo Create it with Python 3.12 or newer, then install the project.
    exit /b 1
)

pushd "%QM_ROOT%" >nul
title QuantMaster
"%QM_PYTHON%" scripts\dev\windows_launcher.py --source "%QM_PYTHON%" --icon "packaging\quantmaster.ico" --output "%QM_LAUNCHER%" >nul 2>nul
if not exist "%QM_LAUNCHER%" (
    echo QuantMaster named launcher could not be prepared; falling back to Python.
    set "QM_LAUNCHER=%QM_PYTHON%"
)
rem Keep cmd.exe out of the long-running app tree.  Once this short launcher
rem exits, QuantMaster becomes the visible root and owns stockdb/workers.
rem Pass --no-reload for the traditional single-process mode.
start "QuantMaster" /b "%QM_LAUNCHER%" -m quantmaster.cli serve --reload %*
set "QM_EXIT_CODE=%ERRORLEVEL%"
popd >nul

exit /b %QM_EXIT_CODE%
