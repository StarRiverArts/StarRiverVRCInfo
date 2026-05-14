@echo off
setlocal
cd /d "%~dp0"
set WORLD_INFO_WEB_FORCE_RESTART=1

where py >nul 2>nul
if %errorlevel%==0 (
  py -3 -m world_info_web.launcher
) else (
  python -m world_info_web.launcher
)

if errorlevel 1 (
  echo.
  echo World Info Web launcher failed.
  echo Backend stdout log: %~dp0world_info_web\data\backend_stdout.log
  echo Backend stderr log: %~dp0world_info_web\data\backend_stderr.log
  pause
)
