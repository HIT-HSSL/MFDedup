//
// Created by Borelset on 2020/5/27.
//
//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_RESTOREREADPIPELINE_H
#define MFDEDUP_RESTOREREADPIPELINE_H

#include <fcntl.h>
#include "RestoreParserPipeline.h"

extern std::string ClassFileAppendPath;

class RestoreReadPipeline {
public:
    RestoreReadPipeline() : taskAmount(0), runningFlag(true), mutexLock(),
                            condition(mutexLock) {
        worker = new std::thread(std::bind(&RestoreReadPipeline::restoreReadCallback, this));
    }

    int addTask(RestoreTask *restoreTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(restoreTask);
        taskAmount++;
        condition.notify();
    }

    ~RestoreReadPipeline() {
        printf("restore read duration :%lu\n", duration);
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

private:
    void restoreReadCallback() {
        RestoreTask *restoreTask;

        struct timeval t0, t1;

        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                restoreTask = taskList.front();
                taskList.pop_front();
            }
            gettimeofday(&t0, NULL);

            std::vector<uint64_t> classList, versionList;
            for (uint64_t i = restoreTask->targetVersion; i <= restoreTask->maxVersion - 1; i++) {
                versionList.push_back(i);
                printf("version # %lu is required\n", i);
            }
            uint64_t baseClass = (restoreTask->maxVersion - 1) * restoreTask->maxVersion / 2 + 1;
            for (uint64_t i = baseClass; i < baseClass + restoreTask->targetVersion; i++) {
                classList.push_back(i);
                printf("class # %lu is required\n", i);
            }
            printf("append class # %lu is optional\n", baseClass);

            for (auto &item : versionList) {
                readFromVersionFile(item, restoreTask->targetVersion);
            }
            for (auto &item : classList) {
                readFromClassFile(item);
            }
            readFromAppendClassFile(baseClass);

            RestoreParseTask *restoreParseTask = new RestoreParseTask(true);
            GlobalRestoreParserPipelinePtr->addTask(restoreParseTask);

            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec-t0.tv_sec)*1000000 + t1.tv_usec - t0.tv_usec;

        }
    }

    int readFromVersionFile(uint64_t versionId, uint64_t restoreVersion) {
        sprintf(filePath, VersionFilePath.data(), versionId);
        FileOperator versionReader(filePath, FileOpenType::Read);
        FILE* versionFileFD = versionReader.getFP();

        VersionFileHeader* versionFileHeader;

        uint64_t leftLength = 0;
        {
            uint8_t *readBuffer = (uint8_t *) malloc(FLAGS_RestoreReadBufferLength);
            uint64_t bytesToRead = FLAGS_RestoreReadBufferLength;
            uint64_t bytesFinallyRead = fread(readBuffer, 1, bytesToRead, versionFileFD);
            versionFileHeader = (VersionFileHeader*)readBuffer;
            uint64_t* offset = (uint64_t*)(readBuffer + sizeof(VersionFileHeader));
            for(int i=0; i<restoreVersion; i++){
                leftLength += offset[i];
            }
            uint64_t totalHeaderLength = sizeof(VersionFileHeader) + versionFileHeader->offsetCount * sizeof(uint64_t);

            if(leftLength < bytesFinallyRead - totalHeaderLength){
                RestoreParseTask* restoreParseTask = new RestoreParseTask(readBuffer, leftLength);
                restoreParseTask->index = versionId;
                restoreParseTask->beginPos = totalHeaderLength;
                GlobalRestoreParserPipelinePtr->addTask(restoreParseTask);
                leftLength = 0;
            }else{
                RestoreParseTask* restoreParseTask = new RestoreParseTask(readBuffer, bytesFinallyRead - totalHeaderLength);
                restoreParseTask->index = versionId;
                restoreParseTask->beginPos = totalHeaderLength;
                GlobalRestoreParserPipelinePtr->addTask(restoreParseTask);
                leftLength -= bytesFinallyRead - totalHeaderLength;
            }
        }

        while (leftLength > 0) {
            uint8_t *readBuffer = (uint8_t *) malloc(FLAGS_RestoreReadBufferLength);
            uint64_t bytesToRead =
                    leftLength > FLAGS_RestoreReadBufferLength ? FLAGS_RestoreReadBufferLength : leftLength;
            uint64_t bytesFinallyRead = fread(readBuffer, 1, bytesToRead, versionFileFD);
            leftLength -= bytesFinallyRead;

            RestoreParseTask* restoreParseTask = new RestoreParseTask(readBuffer, bytesFinallyRead);
            restoreParseTask->index = versionId;
            GlobalRestoreParserPipelinePtr->addTask(restoreParseTask);
        }
    }


    int readFromClassFile(uint64_t classId) {
        sprintf(filePath, ClassFilePath.data(), classId);
        FileOperator classReader(filePath, FileOpenType::Read);
        int fd = classReader.getFd();

        uint64_t leftLength = FileOperator::size(filePath);

        while (leftLength > 0) {
            uint8_t *readBuffer = (uint8_t *) malloc(FLAGS_RestoreReadBufferLength);
            uint64_t bytesToRead =
                    leftLength > FLAGS_RestoreReadBufferLength ? FLAGS_RestoreReadBufferLength : leftLength;
            uint64_t bytesFinallyRead = read(fd, readBuffer, bytesToRead);

            leftLength -= bytesFinallyRead;

            RestoreParseTask *restoreParseTask = new RestoreParseTask(readBuffer, bytesFinallyRead);
            restoreParseTask->index = classId;
            GlobalRestoreParserPipelinePtr->addTask(restoreParseTask);
        }
    }

    int readFromAppendClassFile(uint64_t classId) {
        sprintf(filePath, ClassFileAppendPath.data(), classId);
        FileOperator classReader(filePath, FileOpenType::Read);
        if(classReader.ok()){
            int fd = classReader.getFd();

            uint64_t leftLength = FileOperator::size(filePath);

            while (leftLength > 0) {
                uint8_t *readBuffer = (uint8_t *) malloc(FLAGS_RestoreReadBufferLength);
                uint64_t bytesToRead =
                        leftLength > FLAGS_RestoreReadBufferLength ? FLAGS_RestoreReadBufferLength : leftLength;
                uint64_t bytesFinallyRead = read(fd, readBuffer, bytesToRead);

                leftLength -= bytesFinallyRead;

                RestoreParseTask *restoreParseTask = new RestoreParseTask(readBuffer, bytesFinallyRead);
                restoreParseTask->index = classId;
                GlobalRestoreParserPipelinePtr->addTask(restoreParseTask);
            }
        }
    }


    char filePath[256];
    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<RestoreTask *> taskList;
    MutexLock mutexLock;
    Condition condition;

    uint64_t duration = 0;
};

static RestoreReadPipeline *GlobalRestoreReadPipelinePtr;


#endif //MFDEDUP_RESTOREREADPIPELINE_H
