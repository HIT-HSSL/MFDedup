//
// Created by Borelset on 2019/7/29.
//
//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_READFILEPIPELINE_H
#define MFDEDUP_READFILEPIPELINE_H

#include "jemalloc/jemalloc.h"
#include <sys/time.h>
#include "../Utility/StorageTask.h"
#include "../Utility/FileOperator.h"
#include "ChunkingPipeline.h"

const uint64_t ReadPipelineReadBlockSize = (uint64_t) 128 * 1024 * 1024;

class ReadFilePipeline {
public:
    ReadFilePipeline() : runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock) {
        worker = new std::thread(std::bind(&ReadFilePipeline::readFileCallback, this));
    }

    int addTask(StorageTask *storageTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(storageTask);
        taskAmount++;
        condition.notify();
    }

    ~ReadFilePipeline() {
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

    void getStatistics() {
        printf("Reading Duration : %lu\n", duration);
    }

private:
    void readFileCallback() {
        StorageTask *storageTask;
        struct timeval t0, t1;
        struct timeval rt1, rt2, rt3, rt4;
        ChunkTask chunkTask;
        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                storageTask = taskList.front();
                taskList.pop_front();
            }

            duration = 0;

            CountdownLatch *cd = storageTask->countdownLatch;
            FileOperator fileOperator((char *) storageTask->path.c_str(), FileOpenType::Read);
            storageTask->length = FileOperator::size((char *) storageTask->path.c_str());
            storageTask->buffer = (uint8_t *) malloc(storageTask->length);
            uint64_t readOffset = 0;
            uint64_t readOnce = 0;
            chunkTask.fileID = storageTask->fileID;
            chunkTask.buffer = storageTask->buffer;
            chunkTask.length = storageTask->length;

            gettimeofday(&t0, NULL);
            while (readOnce = fileOperator.read(storageTask->buffer + readOffset, ReadPipelineReadBlockSize)) {
                readOffset += readOnce;
                chunkTask.end = readOffset;
                if (readOnce < ReadPipelineReadBlockSize) {
                    chunkTask.countdownLatch = cd;
                }
                GlobalChunkingPipelinePtr->addTask(chunkTask);

            }
            chunkTask.countdownLatch = nullptr;
            cd->countDown();
            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;

            printf("ReadPipeline finish\n");
            printf("Total read Size:%lu, speed : %fMB/s\n", readOffset,
                   (double) readOffset / ((t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec));

        }
    }

    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<StorageTask *> taskList;
    MutexLock mutexLock;
    Condition condition;
    uint64_t duration = 0;
};

static ReadFilePipeline *GlobalReadPipelinePtr;

#endif //MFDEDUP_READFILEPIPELINE_H
