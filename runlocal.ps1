$ErrorActionPreference = 'Stop'

#region Solution-Wide
function Create-Virtual-Environment() {
    $venvFolder = ".venv"
    if (-not (Test-Path $VenvFolder)) {
        Check-Python-Installed
        Write-Host "Creating Python Virtual Environment"
        python -m venv $VenvFolder
        Activate-Virtual-Environment
    }
}

function Activate-Virtual-Environment() {
    Push-Location "$VenvFolder/Scripts"
    Write-Host "Activating Python Virtual Environment"
    .\activate
    Write-Host "Upgrading pip"
    .\python -m pip install --upgrade pip
    Pop-Location
}

function Check-Python-Installed() {
    $python = cmd /c where python 2> $null
    if (!$python) {
        throw "Python is not installed"
    }
    Write-Host "Python is installed at: $python"
    $windowsStorePython = $python | % { if ($_.Contains("Microsoft\WindowsApps\python.exe")) { return $true } }
    if ([bool]$windowsStorePython) {
        if ($python.Length -gt 1) {
            Write-Warning "Windows Store Python is also installed. It's recommended to remove it"
        }
        else {
            throw "Windows Store Python is the only version installed. Please install Python from the website, winget or chocolatey"
        } 
    }
}

function Start-Azurite() {
    $azurite = cmd /c where azurite 2> $null
    if (!$azurite) {
        throw "azurite is not installed"
    }
    if (-not (Test-Path $Home\\.azurite)) { New-Item -Name .azurite -Path $Home -ItemType Directory }
    Push-Location $Home\\.azurite
    $process = "powershell"   
    $isRunning = [System.Diagnostics.Process]::GetProcessesByName($process) | Select-Object CommandLine | Select-String azurite  
    
    if (!$isRunning) {
        Write-Host "Starting azurite"
        Start-Process $process -ArgumentList "cmd /c azurite" -WindowStyle Minimized
    }
    Pop-Location
}

function Update-VSCode-SettingsJson() {
    $settingsFile = "$vsCodePath\\settings.json"
    $original = Get-Content -Raw -Path $settingsFile | ConvertFrom-Json
    $original.'azureFunctions.projectSubpath' = "$functionName"
    $original.'azureFunctions.deploySubpath' = "$functionName"
    [System.IO.File]::WriteAllLines($settingsFile, @($original | ConvertTo-Json -Depth 100))
}

function Get-Function-Name() {
    gci $PSScriptRoot -Filter '*.pytproj' -Depth 1 | % {
        $functionName = $_.BaseName
        $functionProject = Get-Content -Raw -Path $_.FullName
        if ($functionProject.ToLower().Contains("gcm.azurefunctions.sdk")) {
            return $functionName;
        }
    }
    if (!$functionName) {
        throw "No Azure Functions project was found"
    }
}
#endregion Solution-Wide

#region Project-Wide
function Update-Settings() {
    Write-Host "Running configure.ps1"
    ./configure.ps1 | Out-Null
}

function Install-Pip-Requirements() {
    Write-Host "Installing from $RequirementsFile"
    & "$RootFolder\$VenvFolder\Scripts\python" -m pip install -r .\$RequirementsFile
}

function Add-ExtensionBundle() {
    $hostFile = "$((Get-Location).Path)\$HostFile"
    $toAdd = '{ 
        "extensionBundle": {
            "id": "Microsoft.Azure.Functions.ExtensionBundle",
            "version": "[2.*, 3.0.0)"
        }
    }' | ConvertFrom-Json

    $original = Get-Content -Raw -Path $hostFile | ConvertFrom-Json

    Merge-Json $original $toAdd
    [System.IO.File]::WriteAllLines($hostFile, @($original | ConvertTo-Json -Depth 100))
}

function Merge-Json ($target, $source) {
    $source.psobject.Properties | % {
        if ($_.TypeNameOfValue -eq 'System.Management.Automation.PSCustomObject' -and $target."$($_.Name)" ) {
            Merge-Json $target."$($_.Name)" $_.Value
        }
        else {
            $target | Add-Member -MemberType $_.MemberType -Name $_.Name -Value $_.Value -Force
        }
    }
}

function Copy-Files($fileName) {
    Copy-Item -Path "$artifactsPath\$functionName\$fileName" -Destination "$functionPath\$fileName" -Force
    Write-Host "Copying $fileName"
}

#endregion Project-Wide

#region Variables
$VenvFolder = ".venv"
$RootFolder = $PSScriptRoot
$HostFile = "host.json"
$RequirementsFile = "requirements.txt"
$artifacts = "artifacts"
$artifactsPath = "$PSScriptRoot\\artifacts"
$functionName = Get-Function-Name
$functionPath = "$PSScriptRoot\\$functionName"
$vsCode = ".vscode"
$vsCodePath = "$PSScriptRoot\\$vsCode"
#endregion Variables

#region Run
try { Get-ChildItem $artifacts -Recurse -Exclude "Gcm.ConfigurationLoader.dll" | Remove-Item -Recurse -Force }
catch [System.UnauthorizedAccessException] {
    Write-Error @"
Failed to delete the artifacts folder because files are being used by another process. `
Close any processes that might be using the contents of this folder and try again. `
Exception: $($_.Exception.Message)
"@
}

try { & $PSScriptRoot/Build/build.ps1 -u $false }
catch [System.UnauthorizedAccessException] {
    Write-Warning "Gcm.ConfigurationLoader.dll is being used by another process, continuing..." 
    # in case Gcm.ConfigurationLoader.dll is being used by another process, happens if the user runs it twice
}

Update-VSCode-SettingsJson
Create-Virtual-Environment
Activate-Virtual-Environment
Start-Azurite

Push-Location "$artifactsPath\$functionName"
Update-Settings
Add-ExtensionBundle
Install-Pip-Requirements
@("host.json", "local.settings.json") | % { Copy-Files $_ }
Pop-Location
#endregion Run