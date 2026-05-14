@echo off
setlocal
cd /d "%~dp0"

where py >nul 2>nul
if %errorlevel%==0 (
  py -3 -m world_info.ui
) else (
  python -m world_info.ui
)

if errorlevel 1 (
  echo.
  echo World Info UI failed to start.
  pause
)
