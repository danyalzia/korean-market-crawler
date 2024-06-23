$bytes = (Get-CimInstance -class "cim_physicalmemory" | Measure-Object -Property Capacity -Sum).Sum
$mb = $bytes / 1024 / 1024
$max = $mb - 500
SETX NODE_OPTIONS --max-old-space-size=$max | Out-Null