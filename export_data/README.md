# Why is uploading to `dbloader` so complicated?
Uploading to `dbloader` is straightforward if uploading just one file using the [manual upload method](https://github.com/cmu-hgc-mac/HGC_DB_postgres/tree/main/export_data#manual-upload-to-hgcal-dbloader-spool). The `scp` will request the LXPLUS password, 2FA code, and the LXPLUS password another time. Unfortunately `dbloader-hgcal` (the old version of `dbloader`) can only process one file at a time. Therefore, if we wish to upload multiple XML files to `dbloader`, each file needs to be uploaded with the [manual upload method](https://github.com/cmu-hgc-mac/HGC_DB_postgres/tree/main/export_data#manual-upload-to-hgcal-dbloader-spool) one at a time -- requiring two passwords and 2FA codes each time. 

To skip needing the two passwords and 2FA every single time, we use the [control master method](https://github.com/cmu-hgc-mac/HGC_DB_postgres/tree/main/export_data#method-1-scp-to-dbloader-hgcal-via-lxtunnel) where we leave the LXTUNNEL connection open until `scp` of all files is complete. However, this is still one file at a time and can be quite slow.

An alternative to this is to `scp` the files in bulk to LXPLUS and log into LXPLUS and upload to `dbloader` from there. This is what we do with [mass upload method](https://github.com/cmu-hgc-mac/HGC_DB_postgres/tree/main/export_data#method-2-scp-to-lxplus-and-upload-via-mass_uploadpy) which parallelizes the upload of files to the `dbloader` and allows processing of multiple files at once. This is currently in use in this project.

# Allow SCP to LXPLUS with Two-factor authentication

## Method 1: SCP to dbloader-hgcal via LXTUNNEL
#### Create a control process to dbloader-hgcal via LXPLUS
```
ssh -MNf -o ControlMaster=yes -o ControlPath=~/.ssh/ctrl_dbloader -o ControlPersist=2h -o ProxyJump=USERNAME@lxtunnel.cern.ch USERNAME@dbloader-hgcal
```
#### SCP the file
```
scp -o ProxyJump=USERNAME@lxtunnel.cern.ch -o ControlPath=~/.ssh/ctrl_dbloader /directory_to_the_xml_file/xxx.xml USERNAME@dbloader-hgcal:/home/dbspool/spool/hgc/int2r  
```
#### Exit the control process
```
ssh -O exit -o ControlPath=~/.ssh/ctrl_dbloader USERNAME@ctrl_dbloader
```

## Method 2: SCP to LXPLUS and upload via `mass_upload.py`
#### Create a control process to dbloader-hgcal via LXTUNNEL
```
ssh -MNf -o ControlMaster=yes -o ControlPath=~/.ssh/ctrl_dbloader -o ControlPersist=2h -o ProxyJump=USERNAME@lxtunnel.cern.ch USERNAME@dbloader-hgcal
```
<!-- ssh -MNf -o ControlMaster=yes -o ControlPath=~/.ssh/ctrl_lxplus_dbloader -o ControlPersist=2h -J USERNAME@lxtunnel.cern.ch USERNAME@dbloader-hgcal  -->
#### SCP the file to a temporary directory
```
ssh USERNAME@dbloader-hgcal -o ProxyJump=USERNAME@lxtunnel.cern.ch -o ControlPath=~/.ssh/ctrl_dbloader 'mkdir -p ~/hgc_xml_temp'
scp -o ProxyJump=USERNAME@lxtunnel.cern.ch -o ControlPath=~/.ssh/ctrl_dbloader /directory_to_the_xml_file/xxx.xml USERNAME@dbloader-hgcal:~/hgc_xml_temp
```

#### Upload with `mass_loader.py`
The `mass_loader.py` file is sourced from https://gitlab.cern.ch/hgcal-database/usefull-scripts/-/blob/master/mass_loader.py
```
ssh USERNAME@dbloader-hgcal -o ProxyJump=USERNAME@lxtunnel.cern.ch -o ControlPath=~/.ssh/ctrl_dbloader 'python3 - --int2r ~/hgc_xml_temp/*.xml' < export_data/mass_loader.py
```

#### Exit the control process 
```
ssh -O exit -o ControlPath=~/.ssh/ctrl_dbloader USERNAME@ctrl_dbloader
```


# Manual upload to hgcal-dbloader-spool
Replace `USERNAME` in two places and provide the path of the XML file.
#### INT2R:
```
scp -o ProxyJump=USERNAME@lxtunnel.cern.ch /directory_to_the_xml_file/xxx.xml USERNAME@dbloader-hgcal:/home/dbspool/spool/hgc/int2r/
```
#### CMSR:
```
scp -o ProxyJump=USERNAME@lxtunnel.cern.ch /directory_to_the_xml_file/xxx.xml USERNAME@dbloader-hgcal:/home/dbspool/spool/hgc/cmsr/
```

# Developer Instructions
To run `dbloader_scp_xml.py`: You will require CERN credentials and dbloader access. The script prioritizes upload of "build" files before other types of XMLs.
```
python dbloader_scp_xml.py --help
python dbloader_scp_xml.py --dir . --date 2024-08-27
```
