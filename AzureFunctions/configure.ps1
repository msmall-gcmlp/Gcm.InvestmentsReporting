param ($Environment, $BasePath = $PSScriptRoot)
$ErrorActionPreference = 'Stop'

try {
	([IO.Path]::GetFileNameWithoutExtension($PSCommandPath)) |% { if (Test-Path $BasePath/$_.before.ps1) { if (& {. "$BasePath/$_.before.ps1"} -eq $false) { Write-Host "$_before.ps1 returned false - exiting"; exit $LASTEXITCODE } } } # allow decorating before

	if ($Environment) { $env:Environment = $Environment }
	
	# use ConfigurationLoader to build the configuration
	$configLoaderPath = gci -Path $BasePath -Filter Gcm.ConfigurationLoader.dll -Recurse
	[System.Reflection.Assembly]::LoadFrom($configLoaderPath[0].FullName) | Out-Null
	$configurationBuilder = [Gcm.ConfigurationLoader.ConfigurationDefaults]::get_EnvironmentConfigurationBuilder()
	# default json merge settings
	$jsonMergeSettingsType = ([System.AppDomain]::CurrentDomain.GetAssemblies() |? { $_.GetName().Name -eq "Newtonsoft.Json" }).GetType("Newtonsoft.Json.Linq.JsonMergeSettings")
	$jsonMergeSettings = [System.Activator]::CreateInstance($jsonMergeSettingsType)
	# add settings.base.json
	$configurationBuilder.Add("$BasePath/settings.base.json", [Gcm.ConfigurationLoader.ConfigurationDefaults]::SourcePathsForEnvironment) | Out-Null
	# add settings.ext.*.json
	$extensionConfigs = gci -Path $BasePath -Filter settings.ext.*.json
	foreach ($extConfig in $extensionConfigs) {
		$configurationBuilder.Add($extConfig.FullName, [Gcm.ConfigurationLoader.ConfigurationDefaults]::SourcePathsForEnvironment, $null, $jsonMergeSettings) | Out-Null
	}

	# add settings.app.json
	$configurationBuilder.Add("$BasePath/settings.app.json", $null, $null, $jsonMergeSettings) | Out-Null
	# add settings.env.json
	$configurationBuilder.Add("$BasePath/settings.env.json", [Gcm.ConfigurationLoader.ConfigurationDefaults]::SourcePathsForEnvironment, $null, $jsonMergeSettings) | Out-Null
	# build
	$configuration = $configurationBuilder.BuildDictionaryAsync([hashtable]).get_result()

	# write to final settings file
	Set-Content "$BasePath/local.settings.json" (ConvertTo-Json $configuration -Depth 100)
	
	([IO.Path]::GetFileNameWithoutExtension($PSCommandPath)) |% { if (Test-Path $BasePath/$_.after.ps1) { . "$BasePath/$_.after.ps1" } } # allow decorating after

	return $configuration
} catch {
	throw $_.Exception.ToString()
} 