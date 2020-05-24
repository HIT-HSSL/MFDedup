# MFDedup
A Management Friendly Deduplication Prototype System for Backup

### Requirement:
+ isal_crypto
+ jemalloc
+ openssl
+ glib

### Usage:

+ Write a series of versions into the system
```
MFDedup --task=write --Path=[a file contains a list of files to write]
```
exmaple/filelist is an example for file list. There should not exist any empty line in the file list.
+ Restore a version of from the system
```
MFDedup --task=restore --RestorePath=[where the restored file is to locate] --RestoreRecipe=[which version to restore(1 ~ no. of the last version)] --MaxVersion=[how many versions exists in the system]
```
+ Eliminate the earliest version in the system
```
MFDedup --task=eliminate --MaxVersion=[how many versions exists in the system]
```
+ Check status of the system
```
MFDedup --task=status
```
+ More information
```
MFDedup --help
```


use ```--ClassFilePath=[class file path]``` to specify the class files path, default value is ```/data/MFDedupHome/storageFiles/%lu```  
use ```--VersionFilePath=[version file path]``` to specify the version files path, default value is ```/data/MFDedupHome/storageFiles/%lu```  
use ```--LogicFilePath=[logic file path]``` to specify the logic files (recipes) path, default value is ```/data/MFDedupHome/logicFiles/%lu```


