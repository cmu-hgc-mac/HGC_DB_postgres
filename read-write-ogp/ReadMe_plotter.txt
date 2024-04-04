Open Powershell as Administrator
Type 'plotogp' to open window.
or
Type 'autodb' to open window.

To edit it: open powershell and Type
New-Item -ItemType File -Path $PROFILE -Force
notepad.exe $PROFILE
. $PROFILE


function plot_ogp_py {
    $scriptDirectory = "C:\Users\Admin\Desktop\module_assembly_surveys\offsets\data_processing_and_plotting"
    $pythonFile = "file_selector.py"
    Set-Location -Path $scriptDirectory
    python $pythonFile
}

function auto_upload_ogp_py {
    $scriptDirectory = "C:\Users\Admin\Desktop\module_assembly_surveys\offsets\data_processing_and_plotting"
    $pythonFile = "auto_upload.py"
    Set-Location -Path $scriptDirectory
    python $pythonFile
}

Set-Alias autodb auto_upload_ogp_py
Set-Alias plotogp plot_ogp_py


This is other stuff
psql -U postgres -c 'SHOW config_file'
