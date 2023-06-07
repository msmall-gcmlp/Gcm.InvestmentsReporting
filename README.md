# Gcm.InvestmentsReporting
An example of a python pip that will build and deploy via azure devops and GCM’s msbuild system


# Notes to consider
- You must be on vpn to connect to the GCM pip repository
- The GCM pip repository lives in Azure Devops feeds
- You must set up your python pip.conf file to work with the GCM pip repository (instructions below)
- We use pytest for unit testing in Azure devops pipelines.  Consider using tox locally.
- The azure devops pipelines will build, test, and deploy upon every git push
- You will need a GCM repository for your project, so request one as soon as possible so that you can get the azure pipeline up and running
- This tutorial assumes you already have python version 3 installed.  Current support is python 3.7, 3.8, 3.9
- msbuild requires powershell 5, so all the commands below should be executed inside of a powershell 5 console

# Prerequisites && Assumptions
- Gcm.MSBuild.Sdk set up to build
- have dotnet installed
- have the GCM nuget repository set up locally
- Log into Cisco VPN

# HowTo build this python package locally

1. Log into Cisco VPN

2. Create a virtual env (I won't go into detail as to what a venv is, but we use venv's to do build and deploy scripts, so you should do the same for your project)

Here is documentation on venvs if you need more information about this: https://docs.python.org/3/library/venv.html

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

3. Set up your feed locally so that you are able to pip install packages from the GCM python repository.

a) make sure you have the latest version of pip and keyring so that you can authenticate with the GCM azure devops python repository/feed
```powershell
pip install keyring artifacts-keyring
python -m pip install --upgrade pip
```
b) Add a pip.conf to the root of your .venv directory with this content:
```powershell
[global]
index-url=https://pkgs.dev.azure.com/GCMGrosvenor/_packaging/feed/pypi/simple/
```

If you've decided to not use a venv, then you can add the pip.conf to your global package location: `$APPDATA\pip\pip.ini`

c) you can now use `pip install` to install packages, or put them into the setup.py file.  To install packages via setup.py, run this command in the same directory as setup.py:
```powershell
cd PythonPackage
pip install -e .[dev]
```
Please note that the build script will do this for you, so you can use the build script to iterate instead of doing this yourself.

4. To build the project simply run `.\Build\build.ps1` from the root of your checked out repository (not inside the python package project)

This will run pytest and build your whl file.  The whl artifact lives in the standard directory called `dist\`, which lives in the python project directory.

build.ps1 will also create a file called `VERSION` and auto-populates it with a python package version.  This `VERSION` file is read by setup.py for packaging purposes
Before iterating in step 5, makse sure you have build.ps1 run at least once so that it can generate the first `VERSION` for you

5. If you want to not use build.ps1 for iterating, you can run these commands to build and test your changes.    This is the part that uses the .venv that you've created

```powershell
cd PythonPackage
python setup.py bdist_wheel
pip install -e .
pytest
```

# Additional notes
- The package name will come put to be gcm-investmentsreporting but the python module name is gcm.investmentsreporting
  So you will `pip install gcm-investmentsreporting` but `from gcm.investmentsreporting import say_hello` when using it in other code bases.


# how to clean
black --line-length 75  C:\Code\Reporting\Gcm.InvestmentsReporting\AzureFunctions\DurableFunctionsHttpStart\ C:\Code\Reporting\Gcm.InvestmentsReporting\AzureFunctions\EntityExtractActivity\ C:\Code\Reporting\Gcm.InvestmentsReporting\AzureFunctions\ReportCopyActivity\ C:\Code\Reporting\Gcm.InvestmentsReporting\AzureFunctions\ReportCopyOrchestrator\ C:\Code\Reporting\Gcm.InvestmentsReporting\AzureFunctions\Reporting C:\Code\Reporting\Gcm.InvestmentsReporting\AzureFunctions\ReportOrchestrator\ C:\Code\Reporting\Gcm.InvestmentsReporting\AzureFunctions\ReportPublishActivity\ C:\Code\Reporting\Gcm.InvestmentsReporting\AzureFunctions\ReportRunnerOrchestrator\ C:\Code\Reporting\Gcm.InvestmentsReporting\AzureFunctions\Tests\test_reporting\ C:\Code\Reporting\Gcm.InvestmentsReporting\AzureFunctions\utils