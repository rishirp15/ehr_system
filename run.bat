@echo off
title MediSync Pro Launcher

echo =========================================================
echo ==            MediSync Pro System Launcher             ==
echo =========================================================
echo.
echo This script will:
echo  1. Start all Docker containers in a new window.
echo  2. Wait for the services to initialize.
echo  3. Automatically open the 3 web portals in your browser.
echo.
echo To stop the system, go to the new "Docker Services"
echo command prompt window and press CTRL+C.
echo.
echo =========================================================
pause
echo.

echo [*] Starting Docker services in a new window...
start "Docker Services" cmd /k docker-compose up --build

echo [*] Docker is starting up. The script will now launch the web portals.
python launch_portals.py

echo.
pause