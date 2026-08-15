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
rem Do not use START here: it detaches Ctrl+C from QuantMaster and can leave
rem its workers behind. The foreground QuantMaster process owns every app
rem child; cmd.exe is only the unavoidable shell parent of a .cmd launch.
"%QM_LAUNCHER%" -m quantmaster.cli serve %*
set "QM_EXIT_CODE=%ERRORLEVEL%"
popd >nul

exit /b %QM_EXIT_CODE%
