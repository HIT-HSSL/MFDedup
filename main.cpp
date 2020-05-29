//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#include <iostream>

#include "Pipeline/ReadFilePipeline.h"
#include "RestorePipeline/RestoreReadPipeline.h"
#include "Pipeline/GCPipieline.h"
#include "Pipeline/Eliminater.h"
#include "gflags/gflags.h"
#include <fstream>

DEFINE_string(Path,
              "", "storage path");
DEFINE_string(RestorePath,
              "", "restore path");
DEFINE_uint64(RestoreRecipe,
1, "restore recipe");
DEFINE_string(task,
"", "task type");
DEFINE_string(BatchFilePath,
"", "batch process file path");
DEFINE_uint64(MaxVersion,
0, "");

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    std::string statusStr("status");
    std::string restoreStr("restore");
    std::string writeStr("write");
    std::string eliminateStr("eliminate");

    if (FLAGS_task == writeStr) {

        // pipelines init
        //------------------------------------------------------
        GlobalReadPipelinePtr = new ReadFilePipeline();
        GlobalChunkingPipelinePtr = new ChunkingPipeline();
        GlobalHashingPipelinePtr = new HashingPipeline();
        GlobalDeduplicationPipelinePtr = new DeduplicationPipeline();
        GlobalWriteFilePipelinePtr = new WriteFilePipeline();
        GlobalGCPipelinePtr = new GCPipeline();
        GlobalMetadataManagerPtr = new MetadataManager();
        //------------------------------------------------------

        uint64_t dedupDuration = 0, gcDuration = 0;
        uint64_t totalSize = 0;
        std::ifstream infile;
        infile.open(FLAGS_BatchFilePath);
        std::string subPath;
        uint64_t counter = 1;
        std::cout << "Batch path: " << FLAGS_BatchFilePath << std::endl;
        while (std::getline(infile, subPath)) {
            printf("----------------------------------------------\n");
            printf("Dedup Task: %s\n", subPath.data());
            struct timeval t0, t1;
            gettimeofday(&t0, NULL);

            StorageTask storageTask;
            CountdownLatch countdownLatch(5); // there are 5 pipelines in the workflow of write.
            storageTask.path = subPath;
            storageTask.countdownLatch = &countdownLatch;
            storageTask.fileID = counter;
            GlobalReadPipelinePtr->addTask(&storageTask);
            countdownLatch.wait();

            gettimeofday(&t1, NULL);
            uint64_t singleDedup = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            dedupDuration += singleDedup;
            printf("Task duration:%lu us, Task Size:%lu, Speed:%fMB/s\n", singleDedup, storageTask.length,
                   (float) storageTask.length / singleDedup);
            GlobalReadPipelinePtr->getStatistics();
            GlobalHashingPipelinePtr->getStatistics();
            GlobalChunkingPipelinePtr->getStatistics();
            GlobalDeduplicationPipelinePtr->getStatistics();
            GlobalWriteFilePipelinePtr->getStatistics();
            totalSize += storageTask.length;
            printf("----------------------------------------------\n");
            printf("GC Task: Version %lu\n", counter-1);
            CountdownLatch gcLatch(1);
            GCTask gcTask = {
                    counter - 1, &gcLatch,
            };
            gettimeofday(&t0, NULL);
            GlobalGCPipelinePtr->addTask(&gcTask);
            gcLatch.wait();
            gettimeofday(&t1, NULL);
            uint64_t singleGC = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            gcDuration += singleGC;
            printf("----------------------------------------------\n");
            counter++;
        }

        printf("==============================================\n");
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
        delete GlobalGCPipelinePtr;
        delete GlobalMetadataManagerPtr;
        //------------------------------------------------------

    } else if (FLAGS_task == restoreStr) {

        struct timeval t0, t1;

        char recipePath[256];
        sprintf(recipePath, FLAGS_LogicFilePath.data(), FLAGS_RestoreRecipe);
        CountdownLatch countdownLatch(1);

        RestoreTask restoreTask = {
                FLAGS_MaxVersion,
                FLAGS_RestoreRecipe,
        };

        GlobalRestoreReadPipelinePtr = new RestoreReadPipeline();
        GlobalRestoreWritePipelinePtr = new RestoreWritePipeline(FLAGS_RestorePath, &countdownLatch);  // order is important.
        GlobalRestoreParserPipelinePtr = new RestoreParserPipeline(FLAGS_RestoreRecipe, recipePath);  // order is important.

        gettimeofday(&t0, NULL);
        GlobalRestoreReadPipelinePtr->addTask(&restoreTask);
        countdownLatch.wait();
        gettimeofday(&t1, NULL);
        uint64_t duration = (t1.tv_sec-t0.tv_sec)*1000000 + (t1.tv_usec-t0.tv_usec);
        printf("Total duration : %lu, speed : %f MB/s\n", duration, (float)GlobalRestoreWritePipelinePtr->getTotalSize() / duration);

        delete GlobalRestoreReadPipelinePtr;
        delete GlobalRestoreParserPipelinePtr;
        delete GlobalRestoreWritePipelinePtr;

    } else if (FLAGS_task == eliminateStr) {
        Eliminater eliminater;
        eliminater.run(FLAGS_MaxVersion);
    } else if (FLAGS_task == statusStr) {
        printf("Not implemented\n");
    } else {
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