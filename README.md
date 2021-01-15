# MFDedup
A Management Friendly Deduplication Prototype System for Backup    
Variant B - inline deduplication and offline arrangement

The naming style of categories in this implement (serial number style) is little different from that in the paper (coordinate style).
```
Category X(X-1)/2+Y <= Category(X,Y)
```


### Requirement:
+ isal_crypto
+ jemalloc
+ openssl
+ glib

### Build
```
cd build
cmake ..
make -j 4
./build.sh [working path, identical to "path" in config file.]
```

### Usage:

+ Initializing
```
cd build
chmod +x init.sh
./init.sh [working path, identical to "path" in config file.]
```

+ Backup a new workload into the system, which includes backup workflow, arrangement workflow, and deletion workflow when exceeding the retaining limit.
```
./MFDedup --ConfigFile=[config file path] --task=write --InputFile=[backup workload]
```
build/config.toml is an example for config file.
     
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


