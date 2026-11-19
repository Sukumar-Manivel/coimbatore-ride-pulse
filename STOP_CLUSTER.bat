@echo off
cd /d "%~dp0"
REM Stops the containers. Your HDFS data stays in Docker volumes.
REM To delete everything instead:  docker compose down -v
docker compose stop
pause
