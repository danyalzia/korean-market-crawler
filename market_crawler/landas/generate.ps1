..\..\.venv\Scripts\activate.ps1
.\update.bat
$directory = Split-Path -Path (Get-Location) -Leaf
$date = Get-Date -Format "yyyyMMdd"
$output = $directory.ToUpper() + "_" + $date + ".xlsx"

python ..\..\generate.py `
    --market $directory `
    --column_mapping 'column_mapping.json' `
    --template_file "../../TEMPLATE_DB.xlsx" `
    --output_file $output `
    $args
