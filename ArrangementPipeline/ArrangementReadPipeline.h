//
// Created by Borelset on 2020/6/20.
//
//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_ARRANGEMENTREADPIPELINE_H
#define MFDEDUP_ARRANGEMENTREADPIPELINE_H

#include "ArrangementFilterPipeline.h"
#include "../Utility/FileOperator.h"

extern std::string LogicFilePath;
extern std::string ClassFilePath;
extern std::string VersionFilePath;

class ArrangementReadPipeline{
public:
    ArrangementReadPipeline(): taskAmount(0), runningFlag(true), mutexLock(), condition(mutexLock){
        worker = new std::thread(std::bind(&ArrangementReadPipeline::arrangementReadCallback, this));
    }

    int addTask(ArrangementTask *arrangementTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(arrangementTask);
        taskAmount++;
        condition.notify();
    }

    ~ArrangementReadPipeline() {
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }


private:

    void arrangementReadCallback() {
        ArrangementTask *arrangementTask;

        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                arrangementTask = taskList.front();
                taskList.pop_front();
            }

            uint64_t arrangementVersion = arrangementTask->arrangementVersion;

            if (likely(arrangementVersion > 0)) {

                uint64_t startClass = (arrangementVersion - 1) * (arrangementVersion) / 2 + 1;
                 uint64_t endClass = arrangementVersion * (arrangementVersion + 1) / 2;
                for (uint64_t i = startClass; i <= endClass; i++) {
                    readClass(i, arrangementVersion);
                }
                ArrangementFilterTask* arrangementFilterTask = new ArrangementFilterTask(true);
                arrangementFilterTask->countdownLatch = arrangementTask->countdownLatch;
                GlobalArrangementFilterPipelinePtr->addTask(arrangementFilterTask);
                printf("ArrangementReadPipeline finish\n");
            } else {
                printf("Version 0 does not exist and do not need to GC, skip\n");
                arrangementTask->countdownLatch->countDown();
            }
        }
    }

    uint64_t readClass(uint64_t classId, uint64_t versionId){
        char pathbuffer[512];
        sprintf(pathbuffer, ClassFilePath.data(), classId);
        FileOperator classFile((char *) pathbuffer, FileOpenType::Read);
        while(1){
            uint8_t* buffer = (uint8_t*)malloc(FLAGS_ArrangementReadBufferLength);
            uint64_t readSize = classFile.read(buffer, FLAGS_ArrangementReadBufferLength);
            if(readSize == 0) {
                ArrangementFilterTask* arrangementFilterTask = new ArrangementFilterTask(true, classId);
                GlobalArrangementFilterPipelinePtr->addTask(arrangementFilterTask);
                free(buffer);
                break;
            }
            ArrangementFilterTask* arrangementFilterTask = new ArrangementFilterTask(buffer, readSize, classId, versionId);
            GlobalArrangementFilterPipelinePtr->addTask(arrangementFilterTask);
        }
    }


    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<ArrangementTask *> taskList;
    MutexLock mutexLock;
    Condition condition;
};

static ArrangementReadPipeline* GlobalArrangementReadPipelinePtr;

#endif //MFDEDUP_ARRANGEMENTREADPIPELINE_H
