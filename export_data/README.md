# Manual upload to hgcal-dbloader-spool
Replace `username` in two places and provide the path of the XML file.
#### INT2R:
```
scp -o ProxyJump=username@lxplus.cern.ch /directory_to_the_xml_file/xxx.xml username@dbloader-hgcal:/home/dbspool/spool/hgc/int2r/
```
#### CMSR:
```
scp -o ProxyJump=username@lxplus.cern.ch /directory_to_the_xml_file/xxx.xml username@dbloader-hgcal:/home/dbspool/spool/hgc/cmsr/
```

# Developer Instructions
To run `dbloader_scp_xml.py`: You will require CERN credentials and dbloader access. The script prioritizes upload of "build" files before other types of XMLs.
```
python dbloader_scp_xml.py --help
python dbloader_scp_xml.py --dir . --date 2024-08-27
```
