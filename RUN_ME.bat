@echo off
REM Coimbatore Ride Pulse - one click: build cluster, generate data, run Hadoop jobs, open dashboard.
cd /d "%~dp0"
echo.
echo [1/3] Starting the 4-node Hadoop cluster (first time builds the image, ~5-10 min)...
docker compose up -d --build
if errorlevel 1 goto fail
echo.
echo [2/3] Waiting 30 s for HDFS and YARN to start...
timeout /t 30 /nobreak >nul
echo.
echo [3/3] Generating data and running all MapReduce jobs + benchmarks (30-60 min)...
docker exec master bash -c "sed -i 's/\r$//' /project/scripts/*.sh && bash /project/scripts/run_all.sh"
if errorlevel 1 goto fail
start http://localhost:8000
echo Done. Dashboard: http://localhost:8000
pause
exit /b 0
:fail
echo.
echo Something failed. Is Docker Desktop running? See README.md, section "If something goes wrong".
pause
exit /b 1
