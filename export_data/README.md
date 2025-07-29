# Allow SCP to LXPlus with Two-factor authentication
### Create a control process
```
ssh -MNf -o ControlMaster=yes -o ControlPath=~/.ssh/scp-%r@%h:%p -o ControlPersist=2h -o ProxyJump=USERNAME@lxplus.cern.ch USERNAME@dbloader-hgcal  
```
### SCP the file
```
scp -o ProxyJump=USERNAME@lxplus.cern.ch -o ControlPath=~/.ssh/scp-%r@%h:%p /directory_to_the_xml_file/xxx.xml USERNAME@dbloader-hgcal:/home/dbspool/spool/hgc/int2r  
```
### Exit the control process with the appropriate port
```
ssh -O exit -o ControlPath=~/.ssh/scp-USERNAME@dbloader-hgcal:22 USERNAME@dbloader-hgcal 
```


# Manual upload to hgcal-dbloader-spool
Replace `USERNAME` in two places and provide the path of the XML file.
#### INT2R:
```
scp -o ProxyJump=USERNAME@lxplus.cern.ch /directory_to_the_xml_file/xxx.xml USERNAME@dbloader-hgcal:/home/dbspool/spool/hgc/int2r/
```
#### CMSR:
```
scp -o ProxyJump=USERNAME@lxplus.cern.ch /directory_to_the_xml_file/xxx.xml USERNAME@dbloader-hgcal:/home/dbspool/spool/hgc/cmsr/
```

# Developer Instructions
To run `dbloader_scp_xml.py`: You will require CERN credentials and dbloader access. The script prioritizes upload of "build" files before other types of XMLs.
```
python dbloader_scp_xml.py --help
python dbloader_scp_xml.py --dir . --date 2024-08-27
```
