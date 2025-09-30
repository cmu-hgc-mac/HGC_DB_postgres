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
