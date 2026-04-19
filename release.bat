@echo off
setlocal enabledelayedexpansion

set "ENV_NAME=pixi_313_pd22"
set "RUN_ENV=%USERPROFILE%\bin\python_env_management\run_env.bat"
set "MODE=%~1"
if "%MODE%"=="" set "MODE=all"
set "TAG_FLAG=%~2"
set "CREATED_TAG="

if not exist "%RUN_ENV%" (
    echo ERROR: could not find run_env.bat at %RUN_ENV%
    exit /b 1
)

cd /d "%~dp0"
set "CONDA_BLD_DIR=%~dp0..\ionbus_parquet_cache_conda-bld"

if not "%TAG_FLAG%"=="" if /I not "%TAG_FLAG%"=="--tag" goto usage
if /I "%MODE%"=="build" goto build
if /I "%MODE%"=="send" goto send
if /I "%MODE%"=="all" goto all
:usage
echo Usage: %~nx0 [all^|build^|send] [--tag]
echo   build: build PyPI and conda artifacts locally
echo   send: upload dist/* to PyPI and conda artifacts to ionbus
echo   --tag: create, verify, and optionally push a new git tag before running
exit /b 2

:get_tag
set "GIT_DESCRIBE_TAG="
for /f "usebackq delims=" %%I in (`git describe --tags --exact-match 2^>nul`) do set "GIT_DESCRIBE_TAG=%%I"
if not defined GIT_DESCRIBE_TAG (
    echo ERROR: HEAD is not tagged. Re-run with --tag to create a release tag first.
    exit /b 1
)
exit /b 0

:verify_tag
call :get_tag
if errorlevel 1 exit /b 1
if not "%~1"=="" (
    if /I not "%GIT_DESCRIBE_TAG%"=="%~1" (
        echo ERROR: expected HEAD tag "%~1" but found "%GIT_DESCRIBE_TAG%"
        exit /b 1
    )
)
exit /b 0

:get_conda_build_exe
set "CONDA_BUILD_EXE="
for /f "usebackq delims=" %%I in (`call "%RUN_ENV%" "%ENV_NAME%" where conda-build 2^>nul`) do set "CONDA_BUILD_EXE=%%I"
if defined CONDA_BUILD_EXE exit /b 0
where conda >nul 2>nul
if errorlevel 1 (
    echo ERROR: conda-build is not available in %ENV_NAME% and conda is not on PATH
    exit /b 1
)
set "CONDA_BUILD_EXE=conda"
exit /b 0

:get_conda_output
call :get_conda_build_exe
if errorlevel 1 exit /b 1
set "CONDA_OUTPUT_PATH="
if /I "%CONDA_BUILD_EXE%"=="conda" (
    for /f "usebackq delims=" %%I in (`conda build conda-recipe -c ionbus -c conda-forge --croot "%CONDA_BLD_DIR%" --output`) do set "CONDA_OUTPUT_PATH=%%I"
) else (
    for /f "usebackq delims=" %%I in (`"%CONDA_BUILD_EXE%" conda-recipe -c ionbus -c conda-forge --croot "%CONDA_BLD_DIR%" --output`) do set "CONDA_OUTPUT_PATH=%%I"
)
if not defined CONDA_OUTPUT_PATH (
    echo ERROR: failed to compute conda artifact output path
    exit /b 1
)
exit /b 0

:verify_dist
set "DIST_OK="
for %%F in (dist\*.whl) do (
    echo %%~nxF | findstr /C:"%GIT_DESCRIBE_TAG%" >nul && set "DIST_OK=1"
)
if not defined DIST_OK (
    echo ERROR: expected wheel for tag %GIT_DESCRIBE_TAG% in dist\
    exit /b 1
)
set "DIST_OK="
for %%F in (dist\*.tar.gz) do (
    echo %%~nxF | findstr /C:"%GIT_DESCRIBE_TAG%" >nul && set "DIST_OK=1"
)
if not defined DIST_OK (
    echo ERROR: expected sdist for tag %GIT_DESCRIBE_TAG% in dist\
    exit /b 1
)
exit /b 0

:maybe_tag
if /I not "%TAG_FLAG%"=="--tag" exit /b 0
for /f "usebackq delims=" %%I in (`call "%RUN_ENV%" "%ENV_NAME%" python -m ionbus_utils.git_utils.auto_tag . --name-only`) do set "CREATED_TAG=%%I"
if not defined CREATED_TAG (
    echo ERROR: failed to compute new tag name
    exit /b 1
)
git rev-parse -q --verify "refs/tags/%CREATED_TAG%" >nul 2>nul
if not errorlevel 1 (
    echo ERROR: tag "%CREATED_TAG%" already exists locally
    exit /b 1
)
git tag -a "%CREATED_TAG%" -m "auto-tag %CREATED_TAG%"
if errorlevel 1 exit /b 1
call :verify_tag "%CREATED_TAG%"
if errorlevel 1 exit /b 1
echo Created local tag: %CREATED_TAG%
exit /b %errorlevel%

:build_release
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist "%CONDA_BLD_DIR%" rmdir /s /q "%CONDA_BLD_DIR%"
for /d %%D in (*.egg-info) do rmdir /s /q "%%D"

call :verify_tag
if errorlevel 1 exit /b 1

call "%RUN_ENV%" "%ENV_NAME%" python -c "import build"
if errorlevel 1 (
    call "%RUN_ENV%" "%ENV_NAME%" python setup.py sdist bdist_wheel
    if errorlevel 1 exit /b 1
) else (
    call "%RUN_ENV%" "%ENV_NAME%" python -m build --no-isolation --skip-dependency-check
    if errorlevel 1 (
        call "%RUN_ENV%" "%ENV_NAME%" python setup.py sdist bdist_wheel
        if errorlevel 1 exit /b 1
    )
)

call "%RUN_ENV%" "%ENV_NAME%" python -c "import twine"
if errorlevel 1 (
    echo WARNING: twine is not installed in %ENV_NAME%; skipping twine check
) else (
    call "%RUN_ENV%" "%ENV_NAME%" python -c "import pathlib, subprocess, sys; files=sorted(str(p) for p in pathlib.Path('dist').glob('*')); sys.exit(subprocess.run([sys.executable, '-m', 'twine', 'check', *files], check=False).returncode if files else 1)"
    if errorlevel 1 exit /b 1
)

call :verify_dist
if errorlevel 1 exit /b 1
call :get_conda_output
if errorlevel 1 exit /b 1

if /I "%CONDA_BUILD_EXE%"=="conda" (
    conda build conda-recipe -c ionbus -c conda-forge --croot "%CONDA_BLD_DIR%"
    if errorlevel 1 exit /b 1
) else (
    "%CONDA_BUILD_EXE%" conda-recipe -c ionbus -c conda-forge --croot "%CONDA_BLD_DIR%"
    if errorlevel 1 exit /b 1
)
if not exist "%CONDA_OUTPUT_PATH%" (
    echo ERROR: expected conda artifact was not created: %CONDA_OUTPUT_PATH%
    exit /b 1
)

echo.
echo Built pip artifacts in: %CD%\dist
echo Built conda artifact: %CONDA_OUTPUT_PATH%
echo Version/tag used: %GIT_DESCRIBE_TAG%
exit /b 0

:send_release
call :verify_tag
if errorlevel 1 exit /b 1
call :verify_dist
if errorlevel 1 exit /b 1
call :get_conda_output
if errorlevel 1 exit /b 1
if not exist "%CONDA_OUTPUT_PATH%" (
    echo ERROR: expected conda artifact is missing: %CONDA_OUTPUT_PATH%
    exit /b 1
)

call "%RUN_ENV%" "%ENV_NAME%" python -c "import pathlib, subprocess, sys; files=sorted(str(p) for p in pathlib.Path('dist').glob('*')); sys.exit(subprocess.run([sys.executable, '-m', 'twine', 'upload', *files], check=False).returncode if files else 1)"
if errorlevel 1 exit /b 1

where anaconda >nul 2>nul
if errorlevel 1 (
    echo ERROR: anaconda CLI is required to upload conda packages to ionbus::ionbus-parquet-cache
    exit /b 1
)

anaconda upload -u ionbus "%CONDA_OUTPUT_PATH%"
if errorlevel 1 exit /b 1
exit /b 0

:build
call :maybe_tag
if errorlevel 1 exit /b 1
call :build_release
exit /b %errorlevel%

:send
call :maybe_tag
if errorlevel 1 exit /b 1
call :send_release
exit /b %errorlevel%

:all
call :maybe_tag
if errorlevel 1 exit /b 1
call :build_release
if errorlevel 1 exit /b 1
call :send_release
exit /b %errorlevel%
