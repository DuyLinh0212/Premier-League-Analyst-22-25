# Runs only the API and Web; existing infrastructure is left untouched.
$ErrorActionPreference = 'Stop'
$runnerPython = if ($env:PL_PYTHON_BIN) { $env:PL_PYTHON_BIN } else { 'python' }
& $runnerPython (Join-Path $PSScriptRoot 'run.py') web-backend @args
exit $LASTEXITCODE
