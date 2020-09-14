//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#include <iostream>

#include "DedupPipeline/ReadFilePipeline.h"
#include "RestorePipeline/RestoreReadPipeline.h"
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
DEFINE_bool(ApplyArrangement,
              true, "Whether apply arrangement");

std::string LogicFilePath;
std::string ClassFilePath;
std::string VersionFilePath;
std::string ManifestPath;
std::string HomePath;
std::string ClassFileAppendPath;
uint64_t TotalVersion;
uint64_t RetentionTime;
std::string KVPath;

uint64_t  do_backup(const std::string& path){
    StorageTask storageTask;
    CountdownLatch countdownLatch(5); // there are 5 pipelines in the workflow of write.
    storageTask.path = path;
    storageTask.countdownLatch = &countdownLatch;
    storageTask.fileID = TotalVersion;
    GlobalReadPipelinePtr->addTask(&storageTask);
    countdownLatch.wait();
    return storageTask.length;
}

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

    return 0;
}

int do_arrangement(){
    printf("Arrangement Task: Version %lu\n", TotalVersion-1);
    CountdownLatch arrangementLatch(1);
    ArrangementTask arrangementTask = {
            TotalVersion - 1, &arrangementLatch,
    };
    GlobalArrangementReadPipelinePtr->addTask(&arrangementTask);
    arrangementLatch.wait();
}

int do_delete(){
    printf("------------------------Deleting----------------------\n");
    printf("%lu versions exist, delete the earliest version\n", TotalVersion);
    printf("Delete Task..\n");
    Eliminator eliminator;
    eliminator.run(TotalVersion);
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

    if (FLAGS_task == writeStr) {

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
        std::string workloadPath = FLAGS_InputFile;
        uint64_t taskLength = 0;

        {
            TotalVersion++;
            printf("-----------------------Backing up-----------------------\n");
            printf("Dedup Task: %s\n", workloadPath.data());
            struct timeval t0, t1;
            gettimeofday(&t0, NULL);

            taskLength = do_backup(workloadPath);

            gettimeofday(&t1, NULL);
            uint64_t singleDedup = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            dedupDuration += singleDedup;
            printf("Backup duration:%lu us, Backup Size:%lu, Speed:%fMB/s\n", singleDedup, taskLength,
                   (float) taskLength / singleDedup);
            GlobalReadPipelinePtr->getStatistics();
            GlobalChunkingPipelinePtr->getStatistics();
            GlobalHashingPipelinePtr->getStatistics();
            GlobalDeduplicationPipelinePtr->getStatistics();
            GlobalWriteFilePipelinePtr->getStatistics();

            printf("----------------------Arrangement------------------------\n");
            if (FLAGS_ApplyArrangement){
                gettimeofday(&t0, NULL);
                do_arrangement();
                gettimeofday(&t1, NULL);
                uint64_t singleArr = (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
                arrDuration += singleArr;
                printf("Arrangement duration : %lu\n", singleArr);
            }else{
                printf("Arrangement is disabled by user.\n");
                manifest.ArrangementFallBehind++;
            }

            printf("------------------------Retention----------------------\n");
            if(TotalVersion > RetentionTime){
                do_delete();
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
        printf("Total deduplication duration:%lu us, Total Size:%lu, Speed:%fMB/s\n", dedupDuration, taskLength,
               (float) taskLength / dedupDuration);
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
        printf("Totally %lu versions stored.\n", manifest.TotalVersion);
        printf("Arrangement fall  %lu versions behind.\n", manifest.ArrangementFallBehind);
    }
    else {
        printf("=================================================\n");
        printf("Usage: MFDedup [args..]\n");
        printf("1. Write a series of versions into system\n");
        printf("./MFDedup --ConfigFile=[config file] --task=write --InputFile=[backup workload]\n");
        printf("2. Restore a version of from the system\n");
        printf("./MFDedup --ConfigFile=config.toml --task=restore --RestorePath=[where the restored file is to locate] --RestoreRecipe=[which version to restore(1 ~ no. of the last retained version)]\n");
        printf("3. Check status of the system\n");
        printf("./MFDedup --task=status\n");
        printf("--------------------------------------------------\n");
        printf("more information with --help\n");
        printf("=================================================\n");

    }

}