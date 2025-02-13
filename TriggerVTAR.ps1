# # Define the path to the Python executable
# $pythonPath = "C:\VinnieProjects\ExtractEmail\.venv\Scripts\python.exe"

# # Define the path to the Python script
# $scriptPath = "C:\VinnieProjects\ExtractEmail\01_retrieve.py"

# # Define the path to the log file
# $logPath = "C:\VinnieProjects\ExtractEmail\TriggerVTAR_ps.log"

# # Run the Python script and capture any errors
# try {
#     & $pythonPath $scriptPath 2>&1 | Tee-Object -FilePath $logPath -Append
#     Add-Content -Path $logPath -Value "$(Get-Date): VTAR 01_retrieve.py executed successfully."
# } catch {
#     Add-Content -Path $logPath -Value "$(Get-Date): Error executing VTAR 01_retrieve.py - $_"
# }

# # Optional: Log the execution
# Add-Content -Path $logPath -Value "$(Get-Date): VTAR 01_retrieve.py execution attempt logged."


# Define the path to the Python executable
$pythonPath = "C:\VinnieProjects\VTAR\myenv\Scripts\python.exe"

# Define the path to the Python script
$scriptPath = "C:\VinnieProjects\VTAR\01_retrieve.py"

# Define the path to the log file
$logPath = "C:\VinnieProjects\VTAR\TriggerVTAR_ps.log"

# Function to check if the script is running with elevated privileges
function Test-Admin {
    $currentUser = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
    $currentUser.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

# If not running as admin, restart the script as admin
if (-not (Test-Admin)) {
    Start-Process powershell -Verb runAs -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$PSCommandPath`""
    exit
}

# Run the Python script and capture any errors
try {
    & $pythonPath $scriptPath 2>&1 | Tee-Object -FilePath $logPath -Append
    Add-Content -Path $logPath -Value "$(Get-Date): VTAR 01_retrieve.py executed successfully."
} catch {
    Add-Content -Path $logPath -Value "$(Get-Date): Error executing VTAR 01_retrieve.py - $_"
}

# Optional: Log the execution
Add-Content -Path $logPath -Value "$(Get-Date): VTAR 01_retrieve.py execution attempt logged."