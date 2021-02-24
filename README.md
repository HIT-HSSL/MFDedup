# MFDedup
A Management Friendly Deduplication Prototype System for Backup    
Variant B - inline deduplication and offline arranging

Data deduplication is widely used to reduce the size of backup workloads, but it has the known disadvantage of causing poor data locality, also referred to as the fragmentation problem, which leads to poor restore and garbage collection (GC) performance. Current research has considered writing  duplicates to maintain locality (e.g. rewriting) or caching data in memory or SSD, but fragmentation continues to hurt restore and GC performance.  

Investigating the locality issue, we observed that most duplicate chunks in a backup are directly from its previous backup. We therefore propose a novel management-friendly deduplication framework, called MFDedup, that maintains the locality of backup workloads by using a data classification approach to generate an optimal data layout. Specifically, we use two key techniques: Neighbor-Duplicate-Focus indexing (NDF) and Across-Version-Aware Reorganization scheme (AVAR), to perform duplicate detection against a previous backup and then rearrange chunks with an offline and iterative algorithm into a compact, sequential layout that nearly eliminates random I/O during restoration.

### Requirement
+ isal_crypto
+ jemalloc
+ openssl

### Build
```
cd build
cmake ..
make -j 4
``` 

### Usage

+ Initializing
```
cd build
chmod +x init.sh
./init.sh [working path, identical to "path" in config file.]
```

+ Backup a new workload into the system, which includes backup workflow, arranging workflow, and deletion workflow when exceeding the retaining limit.
```
./MFDedup --ConfigFile=[config file path] --task=write --InputFile=[backup workload]
```
build/config.toml is an example of config file.
     
+ Restore a workload of from the system
```
./MFDedup --ConfigFile=[config file path] --task=restore --RestorePath=[path to restore] --RestoreRecipe=[which version to restore(1 ~ no. of the last retained version)]
```  

+ More information
```
MFDedup --help
```

### Related Publication
+ Xiangyu Zou, Jingsong Yuan, Philip Shilane, Wen Xia, Haijun Zhang, and Xuan Wang,
"<a href="https://www.usenix.org/conference/fast21/presentation/zou">The Dilemma between Deduplication and Locality: Can Both be Achieved?</a>",
in Proceedings of the 19th USENIX Conference on File and Storage Technologies (FAST '21).

### Tips
The naming style of categories in this implement (serial number style) is different from that in our paper (coordinate style).
The mapping relationship is as:
```
Serial Number Style      Coordinate Style
Category X(X-1)/2+Y  <=  Category(X,Y)
```
