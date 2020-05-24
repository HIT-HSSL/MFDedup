//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#include <iostream>

#include "Pipeline/ReadFilePipeline.h"
#include "Pipeline/RestorePipleline.h"
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

    GlobalReadPipelinePtr = new ReadFilePipeline();
    GlobalChunkingPipelinePtr = new ChunkingPipeline();
    GlobalHashingPipelinePtr = new HashingPipeline();
    GlobalDeduplicationPipelinePtr = new DeduplicationPipeline();
    GlobalWriteFilePipelinePtr = new WriteFilePipeline();
    GlobalGCPipelinePtr = new GCPipeline();
    GlobalMetadataManagerPtr = new MetadataManager();
    GlobalRestorePipelinePtr = new RestorePipeline();

    if (FLAGS_task == writeStr) {
        struct timeval total0, total1;
        uint64_t totalSize = 0;
        gettimeofday(&total0, NULL);
        std::ifstream infile;
        infile.open(FLAGS_BatchFilePath);
        std::string subPath;
        uint64_t counter = 1;
        std::cout << "Batch path: " << FLAGS_BatchFilePath << std::endl;
        while (std::getline(infile, subPath)) {
            printf("----------------------------------------------\n");
            std::cout << "Task: " << subPath << std::endl;
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
            float totalDuration = (float) (t1.tv_sec - t0.tv_sec) + (float) (t1.tv_usec - t0.tv_usec) / 1000000;
            printf("Task duration:%fs, Task Size:%lu, Speed:%fMB/s\n", totalDuration, storageTask.length,
                   (float) storageTask.length / totalDuration / 1024 / 1024);
            GlobalReadPipelinePtr->getStatistics();
            GlobalHashingPipelinePtr->getStatistics();
            GlobalChunkingPipelinePtr->getStatistics();
            GlobalDeduplicationPipelinePtr->getStatistics();
            GlobalWriteFilePipelinePtr->getStatistics();
            totalSize += storageTask.length;
            printf("----------------------------------------------\n");
            printf("GC start\n");
            CountdownLatch gcLatch(1);
            GCTask gcTask = {
                    counter - 1, &gcLatch,
            };
            GlobalGCPipelinePtr->addTask(&gcTask);
            gcLatch.wait();
            printf("----------------------------------------------\n");
            counter++;
        }
        gettimeofday(&total1, NULL);
        float duration = (float) (total1.tv_sec - total0.tv_sec) + (float) (total1.tv_usec - total0.tv_usec) / 1000000;

        printf("==============================================\n");
        printf("Total duration:%fs, Total Size:%lu, Speed:%fMB/s\n", duration, totalSize,
               (float) totalSize / duration / 1024 / 1024);
        GlobalDeduplicationPipelinePtr->getStatistics();
        printf("done\n");
        printf("==============================================\n");
    } else if (FLAGS_task == restoreStr) {
        char buffer[256];
        sprintf(buffer, FLAGS_LogicFilePath.data(), FLAGS_RestoreRecipe);
        CountdownLatch countdownLatch(1);
        RestoreTask restoreTask = {
                buffer,
                FLAGS_RestorePath,
                FLAGS_MaxVersion,
                FLAGS_RestoreRecipe,
                &countdownLatch
        };
        GlobalRestorePipelinePtr->addTask(&restoreTask);
        countdownLatch.wait();
    } else if (FLAGS_task == eliminateStr) {
        Eliminater eliminater;
        eliminater.run(FLAGS_MaxVersion);
    } else if (FLAGS_task == statusStr) {
        printf("Not implemented\n");
    } else {
        printf("=================================================\n");
        printf("Usage: MFDedup [args..]\n");
        printf("1. Write a series of versions into system\n");
        printf("MFDedup --task=write --Path=[a files contains a list of files to write]\n");
        printf("2. Restore a version of from the system\n");
        printf("MFDedup --task=restore --RestorePath=[where the restored file is to locate] --RestoreRecipe=[which version to restore(1 ~ no. of the last version)] --MaxVersion=[How many versions exists in the system]\n");
        printf("3. Eliminate the earliest version in the system\n");
        printf("MFDedup --task=eliminate --MaxVersion=[How many versions exists in the system]\n");
        printf("4. Check status of the system\n");
        printf("MFDedup --task=status\n");
        printf("--------------------------------------------------\n");
        printf("use --ClassFilePath=[class file path] to specify the class files path, default value is /data/MFDedupHome/storageFiles/%%lu\n");
        printf("use --VersionFilePath=[version file path] to specify the version files path, default value is /data/MFDedupHome/storageFiles/%%lu\n");
        printf("use --LogicFilePath=[logic file path] to specify the logic files (recipes) path, default value is /data/MFDedupHome/logicFiles/%%lu\n");
        printf("more information with --help\n");
        printf("=================================================\n");

    }

    delete GlobalReadPipelinePtr;
    delete GlobalChunkingPipelinePtr;
    delete GlobalHashingPipelinePtr;
    delete GlobalDeduplicationPipelinePtr;
    delete GlobalWriteFilePipelinePtr;
    delete GlobalRestorePipelinePtr;
    delete GlobalGCPipelinePtr;
    delete GlobalMetadataManagerPtr;
}