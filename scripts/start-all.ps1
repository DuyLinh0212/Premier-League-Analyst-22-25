# Runs from any working directory; forwards options to the shared launcher.
$ErrorActionPreference = 'Stop'
$runnerPython = if ($env:PL_PYTHON_BIN) { $env:PL_PYTHON_BIN } else { 'python' }
& $runnerPython (Join-Path $PSScriptRoot 'run.py') all @args
exit $LASTEXITCODE
