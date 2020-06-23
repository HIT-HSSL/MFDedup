//
// Created by Borelset on 2020/6/23.
//
//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_ARRANGEMENTWRITEPIPELINE_H
#define MFDEDUP_ARRANGEMENTWRITEPIPELINE_H

#include <string>
#include "../Utility/StorageTask.h"
#include "../Utility/Lock.h"
#include "../Utility/Likely.h"
#include <thread>
#include <functional>
#include <sys/time.h>
#include "gflags/gflags.h"

DEFINE_uint64(ArrangementFlushBufferLength,
              67108864, "ArrangementFlushBufferLength");

class ChunkWriter {
public:
    ChunkWriter(FileOperator *fd) : fileOperator(fd) {
        writeBuffer = (uint8_t *) malloc(FLAGS_ArrangementFlushBufferLength);
        writeBufferAvailable = FLAGS_ArrangementFlushBufferLength;
    }

    int writeChunk(uint8_t *chunk, int chunkLen) {
        if (chunkLen > writeBufferAvailable) {
            flush();
        }
        uint8_t *writePoint = writeBuffer + FLAGS_ArrangementFlushBufferLength - writeBufferAvailable;
        memcpy(writePoint, chunk, chunkLen);
        writeBufferAvailable -= chunkLen;
        return 0;
    }

    ~ChunkWriter() {
        flush();
        free(writeBuffer);
    }

private:

    int flush() {
        fileOperator->write(writeBuffer, FLAGS_ArrangementFlushBufferLength - writeBufferAvailable);
        writeBufferAvailable = FLAGS_ArrangementFlushBufferLength;
    }

    uint8_t *writeBuffer;
    int writeBufferAvailable;
    FileOperator *fileOperator;
};

class ArrangementWritePipeline{
public:
    ArrangementWritePipeline(): taskAmount(0), runningFlag(true), mutexLock(), condition(mutexLock){
        worker = new std::thread(std::bind(&ArrangementWritePipeline::arrangementWriteCallback, this));
    }

    int addTask(ArrangementWriteTask* arrangementFilterTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(arrangementFilterTask);
        taskAmount++;
        condition.notify();
    }

    ~ArrangementWritePipeline() {
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

private:
    void arrangementWriteCallback(){
        ArrangementWriteTask* arrangementWriteTask;
        char pathBuffer[256];
        uint64_t* length;
        uint64_t currentVersion = 0;
        uint64_t classIter = 0;
        uint64_t classCounter =0 ;

        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                arrangementWriteTask = taskList.front();
                taskList.pop_front();
            }

            if(arrangementWriteTask->classEndFlag){
                length[classIter] = classCounter;
                classIter++;
                classCounter = 0;
                delete arrangementWriteTask;

                char pathbuffer[512];
                sprintf(pathbuffer, ClassFilePath.data(), arrangementWriteTask->classId);
                remove(pathBuffer);

                continue;
            }

            if(arrangementWriteTask->finalEndFlag){
                delete chunkWriter;
                chunkWriter = nullptr;

                fileOperator->seek(sizeof(VersionFileHeader));
                fileOperator->write((uint8_t *) length, sizeof(uint64_t) * currentVersion);

                fileOperator->fdatasync();
                delete fileOperator;
                fileOperator = nullptr;
                free(length);
                currentVersion = -1;

                arrangementWriteTask->countdownLatch->countDown();
                delete arrangementWriteTask;
                printf("ArrangementWritePipeline finish\n");
                continue;
            }

            if(fileOperator == nullptr){
                sprintf(pathBuffer, VersionFilePath.data(), arrangementWriteTask->versionId);
                fileOperator = new FileOperator(pathBuffer, FileOpenType::Write);
                VersionFileHeader versionFileHeader = {
                        .offsetCount = arrangementWriteTask->versionId
                };
                fileOperator->write((uint8_t*)&versionFileHeader, sizeof(uint64_t));
                fileOperator->seek(sizeof(VersionFileHeader) + sizeof(uint64_t) * versionFileHeader.offsetCount);
                length = (uint64_t*)malloc(sizeof(uint64_t)*versionFileHeader.offsetCount);
                currentVersion = arrangementWriteTask->versionId;
                classIter = 0;
                classCounter = 0;
                chunkWriter = new ChunkWriter(fileOperator);
            }

            chunkWriter->writeChunk(arrangementWriteTask->writeBuffer, arrangementWriteTask->length);
            classCounter += arrangementWriteTask->length;

        }
    }

    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<ArrangementWriteTask*> taskList;
    MutexLock mutexLock;
    Condition condition;

    FileOperator* fileOperator = nullptr;
    ChunkWriter* chunkWriter = nullptr;
};

static ArrangementWritePipeline* GlobalArrangementWritePipelinePtr;

#endif //MFDEDUP_ARRANGEMENTWRITEPIPELINE_H
