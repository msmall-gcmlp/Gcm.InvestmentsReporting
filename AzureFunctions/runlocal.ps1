param (
    [string]$SourcePath = "$PSScriptRoot/../artifacts",
    [string]$TargetPath = $PSScriptRoot)
$ErrorActionPreference = 'Stop'

function Copy-Files($fileName) {
    if (-not (Test-Path fileName)) {
        $foundFiles = @(gci -Path $SourcePath -Filter $fileName -Recurse)
        if (!$foundFiles) {
            # only run build (without sdk update) if file can't be found in source
            ./../Build/build.ps1 -u $false
            $foundFiles = @(gci -Path $SourcePath -Filter $fileName -Recurse)
        }
        Copy-Item -Path $foundFiles[0].FullName -Destination $fileName
        Write-Host "Copying $($foundFiles[0].FullName) into $fileName"
    }
}

function Install-PipRequirements() {
    Write-Host "Installing PIP requirements"
    ../.venv/Scripts/python -m pip install --upgrade pip
    ../.venv/Scripts/python -m pip install -r ./requirements.txt
}

function Update-Settings() {
    Write-Host "Running configure.ps1"
    ./configure.ps1 | Out-Null
}

function Start-Azurite() {
    Write-Host "Starting azurite"
    if (-not (Test-Path $Home\\.azurite)) { New-Item -Name .azurite -Path $Home -ItemType Directory }
    Push-Location $Home\\.azurite
    Start-Process azurite.cmd
    Pop-Location
}

function Add-ExtensionBundle() {
    $hostFile = "$TargetPath\host.json"
    $toAdd = '{ 
        "extensionBundle": {
            "id": "Microsoft.Azure.Functions.ExtensionBundle",
            "version": "[2.*, 3.0.0)"
        }
    }' | ConvertFrom-Json

    $original = Get-Content -Raw -Path $hostFile | ConvertFrom-Json

    Merge-Json $original $toAdd
    $UTF8Only = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllLines($hostFile, @($original | ConvertTo-Json -Depth 100), $UTF8Only)
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

Push-Location $TargetPath

try {
    try {    
        # go search source folder for missing files (typically artifacts)
        @("host.json", "settings.base.json", "configure.ps1", "Gcm.ConfigurationLoader.dll") | % { Copy-Files $_ }
    }
    catch {
        # in case Gcm.ConfigurationLoader.dll is being used by another process, happens if the user runs it twice
    }
    Install-PipRequirements
    Update-Settings
    Copy-Files "local.settings.json"
    Add-ExtensionBundle
    Start-Azurite
}
finally { Pop-Location }