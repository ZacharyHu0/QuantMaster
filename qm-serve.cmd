@echo off
setlocal

call "%~dp0scripts\dev\serve.cmd" %*
exit /b %ERRORLEVEL%
