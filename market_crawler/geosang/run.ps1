..\..\.venv\Scripts\activate.ps1
..\..\set_node_heap_memory.ps1
.\update.bat
$directory = Split-Path -Path (Get-Location) -Leaf
$date = Get-Date -Format "yyyyMMdd"
$output = $directory.ToUpper() + "_" + $date + ".xlsx"

python ..\..\run.py `
    --market $directory `
    --column_mapping 'column_mapping.json' `
    --template_file "../../TEMPLATE_DB.xlsx" `
    --output_file $output `
    --detailed_images_html_source_top '<div align="center"></div>' `
    --detailed_images_html_source_bottom '<div align="center"></div>' `
    $args