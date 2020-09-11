//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#include <iostream>

#include "DedupPipeline/ReadFilePipeline.h"
#include "RestorePipeline/RestoreReadPipeline.h"
//#include "ArrangementPipeline/GCPipieline.h"
#include "DedupPipeline/Eliminator.h"
#include "gflags/gflags.h"
#include "Utility/Config.h"
#include "Utility/Manifest.h"
#include "ArrangementPipeline/ArrangementReadPipeline.h"

DEFINE_string(RestorePath,
              "", "restore path");
DEFINE_uint64(RestoreRecipe,
1, "restore recipe");
DEFINE_string(task,
"", "task type");
DEFINE_string(BatchFilePath,
"", "batch process file path");
DEFINE_string(ConfigFile,
        "", "config path");
DEFINE_string(InputFile,
              "", "input path");

std::string LogicFilePath;
std::string ClassFilePath;
std::string VersionFilePath;
std::string ManifestPath;
std::string HomePath;
std::string ClassFileAppendPath;
uint64_t TotalVersion;
uint64_t RetentionTime;
std::string KVPath;

int do_restore(uint64_t version){
    struct timeval t0, t1;

    char recipePath[256];
    sprintf(recipePath, LogicFilePath.data(), version);
    CountdownLatch countdownLatch(1);

    RestoreTask restoreTask = {
            TotalVersion,
            version,
    };

    GlobalRestoreReadPipelinePtr = new RestoreReadPipeline();
    GlobalRestoreWritePipelinePtr = new RestoreWritePipeline(FLAGS_RestorePath, &countdownLatch);  // order is important.
    GlobalRestoreParserPipelinePtr = new RestoreParserPipeline(version, recipePath);  // order is important.

    gettimeofday(&t0, NULL);
    GlobalRestoreReadPipelinePtr->addTask(&restoreTask);
    countdownLatch.wait();
    gettimeofday(&t1, NULL);
    uint64_t duration = (t1.tv_sec-t0.tv_sec)*1000000 + (t1.tv_usec-t0.tv_usec);
    printf("Total duration : %lu, speed : %f MB/s\n", duration, (float)GlobalRestoreWritePipelinePtr->getTotalSize() / duration);

    delete GlobalRestoreReadPipelinePtr;
    delete GlobalRestoreParserPipelinePtr;
    delete GlobalRestoreWritePipelinePtr;

}

int do_delete(){
    printf("------------------------Deleting----------------------\n");
    printf("%lu versions exist, delete the earliest version\n", TotalVersion);
    printf("Delete Task..\n");
    Eliminator eliminator;
    eliminator.run(TotalVersion);
    GlobalMetadataManagerPtr->updateMetaTableAfterDeletion();
    TotalVersion--;
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    std::string statusStr("status");
    std::string restoreStr("restore");
    std::string writeStr("write");
    std::string batchStr("batch");
    std::string eliminateStr("delete");

    Manifest manifest;
    {
        ConfigReader configReader(FLAGS_ConfigFile);
        ManifestReader manifestReader(&manifest);
        TotalVersion = manifest.TotalVersion;
    }

    if (FLAGS_task == batchStr) {

        // pipelines init
        //------------------------------------------------------
        GlobalReadPipelinePtr = new ReadFilePipeline();
        GlobalChunkingPipelinePtr = new ChunkingPipeline();
        GlobalHashingPipelinePtr = new HashingPipeline();
        GlobalDeduplicationPipelinePtr = new DeduplicationPipeline();
        GlobalWriteFilePipelinePtr = new WriteFilePipeline();
        GlobalMetadataManagerPtr = new MetadataManager();
        GlobalArrangementReadPipelinePtr = new ArrangementReadPipeline();
        GlobalArrangementFilterPipelinePtr = new ArrangementFilterPipeline();
        GlobalArrangementWritePipelinePtr = new ArrangementWritePipeline();
        //------------------------------------------------------

        uint64_t dedupDuration = 0, arrDuration = 0;
        uint64_t totalSize = 0;
        std::ifstream infile;
        infile.open(FLAGS_BatchFilePath);
        std::string subPath;
        std::cout << "Batch path: " << FLAGS_BatchFilePath << std::endl;
        while (std::getline(infile, subPath)) {
            TotalVersion++;
            printf("----------------------------------------------\n");
            printf("Dedup Task: %s\n", subPath.data());
            struct timeval t0, t1;
            gettimeofday(&t0, NULL);

             StorageTask storageTask;
            CountdownLatch countdownLatch(5); // there are 5 pipelines in the workflow of write.
            storageTask.path = subPath;
            storageTask.countdownLatch = &countdownLatch;
            storageTask.fileID = TotalVersion;
            GlobalReadPipelinePtr->addTask(&storageTask);
            countdownLatch.wait();

            gettimeofday(&t1, NULL);
            uint64_t singleDedup = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            dedupDuration += singleDedup;
            printf("Task duration:%lu us, Task Size:%lu, Speed:%fMB/s\n", singleDedup, storageTask.length,
                   (float) storageTask.length / singleDedup);
            GlobalReadPipelinePtr->getStatistics();
            GlobalChunkingPipelinePtr->getStatistics();
            GlobalHashingPipelinePtr->getStatistics();
            GlobalDeduplicationPipelinePtr->getStatistics();
            GlobalWriteFilePipelinePtr->getStatistics();
            totalSize += storageTask.length;

            printf("----------------------------------------------\n");
            printf("GC Task: Version %lu\n", TotalVersion-1);
            CountdownLatch arrangementLatch(1);
            ArrangementTask arrangementTask = {
                    TotalVersion - 1, &arrangementLatch,
            };
            gettimeofday(&t0, NULL);
            GlobalArrangementReadPipelinePtr->addTask(&arrangementTask);
            arrangementLatch.wait();
            gettimeofday(&t1, NULL);
            uint64_t singleGC = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            arrDuration += singleGC;
            printf("----------------------------------------------\n");

            if(TotalVersion > RetentionTime){
                printf("----------------------------------------------\n");
                printf("%lu versions exist, delete the earliest version\n", TotalVersion);
                printf("Delete Task..\n");
                Eliminator eliminator;
                eliminator.run(TotalVersion);
                GlobalMetadataManagerPtr->updateMetaTableAfterDeletion();
                TotalVersion--;
                printf("----------------------------------------------\n");
            }else{
                printf("Only %lu versions exist, and the retention is %lu, deletion is not required.\n", TotalVersion, RetentionTime);
            }
        }
        {
            manifest.TotalVersion = TotalVersion;
            ManifestWriter manifestWriter(manifest);
            GlobalMetadataManagerPtr->save();
        }

        printf("==============================================\n");
        printf("writing duration:%lu, arrange duration:%lu\n", dedupDuration, arrDuration);
        printf("Total deduplication duration:%fs, Total Size:%lu, Speed:%fMB/s\n", dedupDuration, totalSize,
               (float) totalSize / dedupDuration);
        GlobalDeduplicationPipelinePtr->getStatistics();
        printf("done\n");
        printf("==============================================\n");

        // pipelines release
        //------------------------------------------------------
        delete GlobalReadPipelinePtr;
        delete GlobalChunkingPipelinePtr;
        delete GlobalHashingPipelinePtr;
        delete GlobalDeduplicationPipelinePtr;
        delete GlobalWriteFilePipelinePtr;
        delete GlobalMetadataManagerPtr;
        delete GlobalArrangementReadPipelinePtr;
        delete GlobalArrangementFilterPipelinePtr;
        delete GlobalArrangementWritePipelinePtr;
        //------------------------------------------------------

    }
    else if (FLAGS_task == writeStr) {

        // pipelines init
        //------------------------------------------------------
        GlobalReadPipelinePtr = new ReadFilePipeline();
        GlobalChunkingPipelinePtr = new ChunkingPipeline();
        GlobalHashingPipelinePtr = new HashingPipeline();
        GlobalDeduplicationPipelinePtr = new DeduplicationPipeline();
        GlobalWriteFilePipelinePtr = new WriteFilePipeline();
        GlobalMetadataManagerPtr = new MetadataManager();
        GlobalArrangementReadPipelinePtr = new ArrangementReadPipeline();
        GlobalArrangementFilterPipelinePtr = new ArrangementFilterPipeline();
        GlobalArrangementWritePipelinePtr = new ArrangementWritePipeline();
        //------------------------------------------------------

        if(TotalVersion != 0)
            GlobalMetadataManagerPtr->load();

        uint64_t dedupDuration = 0, arrDuration = 0;
        uint64_t totalSize = 0;
        std::string subPath = FLAGS_InputFile;

        {
            TotalVersion++;
            printf("-----------------------Backing up-----------------------\n");
            printf("Dedup Task: %s\n", subPath.data());
            struct timeval t0, t1;
            gettimeofday(&t0, NULL);

            StorageTask storageTask;
            CountdownLatch countdownLatch(5); // there are 5 pipelines in the workflow of write.
            storageTask.path = subPath;
            storageTask.countdownLatch = &countdownLatch;
            storageTask.fileID = TotalVersion;
            GlobalReadPipelinePtr->addTask(&storageTask);
            countdownLatch.wait();

            gettimeofday(&t1, NULL);
            uint64_t singleDedup = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            dedupDuration += singleDedup;
            printf("Backup duration:%lu us, Backup Size:%lu, Speed:%fMB/s\n", singleDedup, storageTask.length,
                   (float) storageTask.length / singleDedup);
            GlobalReadPipelinePtr->getStatistics();
            GlobalChunkingPipelinePtr->getStatistics();
            GlobalHashingPipelinePtr->getStatistics();
            GlobalDeduplicationPipelinePtr->getStatistics();
            GlobalWriteFilePipelinePtr->getStatistics();
            totalSize += storageTask.length;

            printf("----------------------Arrangement------------------------\n");
            printf("Arrangement Task: Version %lu\n", TotalVersion-1);
            CountdownLatch arrangementLatch(1);
            ArrangementTask arrangementTask = {
                    TotalVersion - 1, &arrangementLatch,
            };
            gettimeofday(&t0, NULL);
            GlobalArrangementReadPipelinePtr->addTask(&arrangementTask);
            arrangementLatch.wait();
            gettimeofday(&t1, NULL);
            uint64_t singleArr = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            arrDuration += singleArr;
            printf("Arrangement duration : %lu\n", singleArr);

            printf("------------------------Retention----------------------\n");
            if(TotalVersion > RetentionTime){

                printf("%lu versions exist, delete the earliest version\n", TotalVersion);
                printf("Delete Task..\n");
                Eliminator eliminator;
                eliminator.run(TotalVersion);
                GlobalMetadataManagerPtr->updateMetaTableAfterDeletion();
                TotalVersion--;
            }else{
                printf("Only %lu versions exist, and the retention is %lu, deletion is not required.\n", TotalVersion, RetentionTime);
            }
        }

        {
            manifest.TotalVersion = TotalVersion;
            ManifestWriter manifestWriter(manifest);
            GlobalMetadataManagerPtr->save();
        }

        printf("==============================================\n");
        printf("writing duration:%lu, arrange duration:%lu\n", dedupDuration, arrDuration);
        printf("Total deduplication duration:%lu us, Total Size:%lu, Speed:%fMB/s\n", dedupDuration, totalSize,
               (float) totalSize / dedupDuration);
        GlobalDeduplicationPipelinePtr->getStatistics();
        printf("done\n");
        printf("==============================================\n");

        // pipelines release
        //------------------------------------------------------
        delete GlobalReadPipelinePtr;
        delete GlobalChunkingPipelinePtr;
        delete GlobalHashingPipelinePtr;
        delete GlobalDeduplicationPipelinePtr;
        delete GlobalWriteFilePipelinePtr;
        delete GlobalMetadataManagerPtr;
        delete GlobalArrangementReadPipelinePtr;
        delete GlobalArrangementFilterPipelinePtr;
        delete GlobalArrangementWritePipelinePtr;
        //------------------------------------------------------

    }
    else if (FLAGS_task == restoreStr) {
        do_restore(FLAGS_RestoreRecipe);
    }
    else if (FLAGS_task == eliminateStr) {
        Eliminator eliminator;
        eliminator.run(TotalVersion);
        TotalVersion--;
        {
            manifest.TotalVersion = TotalVersion;
            ManifestWriter manifestWriter(manifest);
        }
    }
    else if (FLAGS_task == statusStr) {
        printf("Not implemented\n");
    }
    else {
        printf("=================================================\n");
        printf("Usage: MFDedup [args..]\n");
        printf("1. Write a series of versions into system\n");
        printf("MFDedup --task=write --Path=[a file contains a list of files to write]\n");
        printf("2. Restore a version of from the system\n");
        printf("MFDedup --task=restore --RestorePath=[where the restored file is to locate] --RestoreRecipe=[which version to restore(1 ~ no. of the last version)] --MaxVersion=[How many versions exist in the system]\n");
        printf("3. Eliminate the earliest version in the system\n");
        printf("MFDedup --task=eliminate --MaxVersion=[How many versions exist in the system]\n");
        printf("4. Check status of the system\n");
        printf("MFDedup --task=status\n");
        printf("--------------------------------------------------\n");
        printf("use --ClassFilePath=[class file path] to specify the class files path, default value is /data/MFDedupHome/storageFiles/%%lu\n");
        printf("use --VersionFilePath=[version file path] to specify the version files path, default value is /data/MFDedupHome/storageFiles/v%%lu\n");
        printf("use --LogicFilePath=[logic file path] to specify the logic files (recipes) path, default value is /data/MFDedupHome/logicFiles/%%lu\n");
        printf("more information with --help\n");
        printf("=================================================\n");

    }

}