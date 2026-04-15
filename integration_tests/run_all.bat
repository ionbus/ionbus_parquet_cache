@echo off
REM Run all integration tests (Windows)
REM
REM Usage:
REM   run_all.bat           - Run all tests
REM   run_all.bat --skip-setup  - Skip data setup

setlocal enabledelayedexpansion

set SCRIPT_DIR=%~dp0
set SKIP_SETUP=0

:parse_args
if "%~1"=="" goto :done_args
if "%~1"=="--skip-setup" set SKIP_SETUP=1
shift
goto :parse_args
:done_args

echo ========================================
echo   ionbus_parquet_cache Integration Tests
echo ========================================
echo.

REM Use Git Bash to run the shell scripts
set GIT_BASH=C:\Program Files\Git\bin\bash.exe

if not exist "%GIT_BASH%" (
    echo ERROR: Git Bash not found at %GIT_BASH%
    echo Please install Git for Windows or update the path.
    exit /b 1
)

if %SKIP_SETUP%==0 (
    echo ^>^>^> Running 01_setup_data.sh
    "%GIT_BASH%" "%SCRIPT_DIR%01_setup_data.sh"
    if errorlevel 1 exit /b 1
    echo.
)

echo ^>^>^> Running 02_initial_load.sh
"%GIT_BASH%" "%SCRIPT_DIR%02_initial_load.sh"
if errorlevel 1 exit /b 1
echo.

echo ^>^>^> Running 03_incremental_update.sh
"%GIT_BASH%" "%SCRIPT_DIR%03_incremental_update.sh"
if errorlevel 1 exit /b 1
echo.

echo ^>^>^> Running 04_copy_rename.sh
"%GIT_BASH%" "%SCRIPT_DIR%04_copy_rename.sh"
if errorlevel 1 exit /b 1
echo.

echo ^>^>^> Running 05_restate.sh
"%GIT_BASH%" "%SCRIPT_DIR%05_restate.sh"
if errorlevel 1 exit /b 1
echo.

echo ^>^>^> Running 06_preserve_config.sh
"%GIT_BASH%" "%SCRIPT_DIR%06_preserve_config.sh"
if errorlevel 1 exit /b 1
echo.

echo ========================================
echo   All Integration Tests Complete!
echo ========================================
