[CmdletBinding(PositionalBinding = $false)]
param($Sdk = "Gcm.MSBuild.Sdk", $SdkVersion = "1.*", $SdkInstallArgs, $DotNetSdkVersion, [Alias('u')]$UpdateSdk = $true, [Alias('c')]$Configuration = 'Release', [Alias('o')]$Output = "$PSScriptRoot/../artifacts", [Alias('v')]$Verbosity = 'n', [Parameter(ValueFromRemainingArguments)][array]$CommandArgs)
$ErrorActionPreference = 'Stop'
function exec { & $args[0] ($args | Select -Skip 1 |% { $_ }); if ($LASTEXITCODE -ne 0) { throw "Error executing $($args |% { $_ })`nExited with code $LASTEXITCODE" } } # Wrapper for execute command that flattens args and checks return code
Push-Location $PSScriptRoot/..
try {
  # Overwrite the dotnet sdk version built by gcm.msbuild.sdk. Throw error if version not installed
  # In order to use a previous version of the sdk on the automated build agents, the environment variable DOTNET_SKIP_FIRST_TIME_EXPERIENCE must be set to true
  if ($DotNetSdkVersion) { 
    $installedDotNetVersions = (dotnet --list-sdks)
    if(![bool]($installedDotNetVersions -match "$DotNetSdkVersion "))
    {
        throw "The dotnet version $DotNetSdkVersion was specified, but that version is not installed on the computer. Please install and re-run."
    }  
    $globalJsonpath = ".\global.json"
    $globalJsonContent = ConvertFrom-Json (Get-Content $globalJsonpath -Raw )
    if($globalJsonContent.PSObject.Properties.Name -notcontains 'sdk') { Add-Member -InputObject $globalJsonContent -Name "sdk" -Value @{"version"=$DotNetSdkVersion} -MemberType NoteProperty } else {$globalJsonContent.sdk.version = $DotNetSdkVersion}
    ConvertTo-Json $globalJsonContent -Depth 100 | Set-Content $globalJsonpath -Force
  }

  if ($UpdateSdk -and $Sdk -ne '' -and $SdkVersion -ne '') { # update sdk template - at sdk template install time, $SdkVersion will be replaced with a real one (which may be floating, and floating versions aren't supported for sdks in global.json
    exec dotnet new -i "$Sdk::$SdkVersion" $SdkInstallArgs # force no caching using debug flag
    exec dotnet new $Sdk --sdkVersionRange $SdkVersion --force # reinstall and re-run sdk template
    & $PSCommandPath @PSBoundParameters -UpdateSdk $false # re-run this script but don't update sdk template next time
  } else {
    ([IO.Path]::GetFileNameWithoutExtension($PSCommandPath)) |% { if (Test-Path $PSScriptRoot/$_.before.ps1) { if ((. {. "$PSScriptRoot/$_.before.ps1"}) -eq $false) { Write-Host "$_before.ps1 returned false - exiting"; exit $LASTEXITCODE } } } # allow decorating before
    if (Test-Path $Output) { rd $Output -r -Force } # Need -force in removing output as if the folder contains symlinks they will cause a prompt
    md $Output -Force | Out-Null # remove and recreate artifacts dir for build output
    exec dotnet clean -c $Configuration /p:PackageOutputPath=$Output -v $Verbosity $CommandArgs
    # Pack before Build was originally set intentionally to ensure packages are placed in the artifacts folder when building from build.ps1 and in the bin folder when building from Visual Studio.
    # This has been remediated with a different solution with version 1.4 (using property UsePerProjectPackageOutputPath set here), 
    # however the order was kept Pack before Build as some projects have targets that depend on this order. The order is being set to Build before pack as the next major release
    exec dotnet msbuild /r '/t:Pack;Build' /v:$Verbosity /p:Configuration=$Configuration /p:PackageOutputPath=$Output /p:UsePerProjectPackageOutputPath=true $CommandArgs
    ([IO.Path]::GetFileNameWithoutExtension($PSCommandPath)) |% { if (Test-Path $PSScriptRoot/$_.after.ps1) { . "$PSScriptRoot/$_.after.ps1" } } # allow decorating after
  }
} finally { Pop-Location }