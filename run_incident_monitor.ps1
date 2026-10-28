# Runs a single poll of the Ollama incident monitor and appends the output to a log.
# Invoked every 10 minutes by the "ServiceNow Incident Monitor" scheduled task.
$ErrorActionPreference = 'Continue'
Set-Location -Path $PSScriptRoot

$log = Join-Path $PSScriptRoot 'incident_monitor_scheduled.log'
"===== $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') =====" | Out-File -FilePath $log -Append -Encoding utf8

$env:PYTHONIOENCODING = 'utf-8'
& 'C:\Users\SrividhyaGanesan\AppData\Local\Programs\Python\Python311\python.exe' `
    -m servicenow_jira_agent.incident_monitor_ollama *>&1 |
    Out-File -FilePath $log -Append -Encoding utf8
