# MFDedup
A Management Friendly Deduplication Prototype System for Backup    
Variant B - inline deduplication and offline arrangement

### Requirement:
+ isal_crypto
+ jemalloc
+ openssl
+ glib

### Build
```
mkdir build
cd build
cmake ..
make -j 4
```

### Usage:

+ Backup a new workload into the system, which includes backup workflow and arrangement workflow.
```
./MFDedup --ConfigFile=[config file] --task=write --InputFile=[backup workload]
```
exmaple/config.toml is an example for config file.
+ Restore a version of from the system
```
./MFDedup --ConfigFile=config.toml --task=restore --RestorePath=[where the restored file is to locate] --RestoreRecipe=[which version to restore(1 ~ no. of the last retained version)]
```
+ Check status of the system (Not implement)
```
MFDedup --task=status
```
+ More information
```
MFDedup --help
```


