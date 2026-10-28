# Runs a single poll of the Ollama incident monitor and appends the output to a log.
# Invoked every 10 minutes by the "ServiceNow Incident Monitor" scheduled task.
# Output is written line by line as the monitor runs, so the log can be followed live with:
#   Get-Content .\incident_monitor_scheduled.log -Wait -Tail 30
$ErrorActionPreference = 'Continue'
Set-Location -Path $PSScriptRoot

$log = Join-Path $PSScriptRoot 'incident_monitor_scheduled.log'
"===== $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') =====" | Out-File -FilePath $log -Append -Encoding utf8

$env:PYTHONIOENCODING = 'utf-8'
$env:PYTHONUNBUFFERED = '1'   # flush each print immediately instead of at exit
& 'C:\Users\SrividhyaGanesan\AppData\Local\Programs\Python\Python311\python.exe' `
    -m servicenow_jira_agent.incident_monitor_ollama *>&1 |
    ForEach-Object {
        $line = "$_"   # plain text for stderr lines too, without PowerShell error formatting
        $line | Out-File -FilePath $log -Append -Encoding utf8
        $line          # also shown in the console when the script is run by hand
    }
