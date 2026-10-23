@echo off
rem Launcher for cbm-graph.ps1 that works from cmd.exe and PowerShell alike
rem (a .ps1 double-clicked or typed in cmd opens in an editor instead of running).
rem Usage: cbm-graph -RepoPath "D:\path\to\repo" [-Name name] [-Fresh]
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0cbm-graph.ps1" %*
exit /b %ERRORLEVEL%
