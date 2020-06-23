//
// Created by Borelset on 2020/6/22.
//
//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2


#ifndef MFDEDUP_ARRANGEMENTFILTERPIPELINE_H
#define MFDEDUP_ARRANGEMENTFILTERPIPELINE_H

#include "ArrangementWritePipeline.h"
#include "../MetadataManager/MetadataManager.h"

DEFINE_uint64(ArrangementReadBufferLength,
              8388608, "ArrangementBufferLength");

class ArrangementFilterPipeline{
public:
    ArrangementFilterPipeline(): taskAmount(0), runningFlag(true), mutexLock(), condition(mutexLock){
        worker = new std::thread(std::bind(&ArrangementFilterPipeline::arrangementFilterCallback, this));
    }

    int addTask(ArrangementFilterTask* arrangementFilterTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(arrangementFilterTask);
        taskAmount++;
        condition.notify();
    }

    ~ArrangementFilterPipeline() {
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

private:

    void arrangementFilterCallback(){
        ArrangementFilterTask* arrangementFilterTask;
        uint8_t* temp = (uint8_t*)malloc(FLAGS_ArrangementReadBufferLength);

        uint64_t taskRemain = 0;
        uint64_t leftLength = 0;
        uint64_t copyLength = 0;
        BlockHeader *blockHeader;

        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {

                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                arrangementFilterTask = taskList.front();
                taskList.pop_front();
            }

            if(unlikely(arrangementFilterTask->classEndFlag)){
                ArrangementWriteTask* arrangementWriteTask = new ArrangementWriteTask(true, arrangementFilterTask->classId);
                GlobalArrangementWritePipelinePtr->addTask(arrangementWriteTask);
                delete arrangementFilterTask;
                continue;
            }

            if(unlikely(arrangementFilterTask->finalEndFlag)){
                ArrangementWriteTask* arrangementWriteTask = new ArrangementWriteTask(true);
                arrangementWriteTask->countdownLatch = arrangementFilterTask->countdownLatch;
                GlobalArrangementWritePipelinePtr->addTask(arrangementWriteTask);
                delete arrangementFilterTask;
                printf("ArrangementFilterPipeline finish\n");
                continue;
            }

            if (leftLength + arrangementFilterTask->length > FLAGS_ArrangementReadBufferLength) {
                copyLength = FLAGS_ArrangementReadBufferLength - leftLength;
                taskRemain = arrangementFilterTask->length - copyLength;
            } else {
                copyLength = arrangementFilterTask->length;
                taskRemain = 0;
            }
            uint64_t parseLeft = copyLength + leftLength;
            memcpy(temp + leftLength, arrangementFilterTask->readBuffer, copyLength);

            uint64_t readoffset = 0;

            while (1) {
                blockHeader = (BlockHeader *) (temp + readoffset);
                if (parseLeft < sizeof(BlockHeader) || parseLeft < sizeof(BlockHeader) + blockHeader->length) {
                    memcpy(temp, temp + readoffset, parseLeft);
                    memcpy(temp + parseLeft, arrangementFilterTask->readBuffer + copyLength, taskRemain);
                    readoffset = 0;
                    parseLeft += taskRemain;

                    taskRemain = 0;
                    copyLength += taskRemain;
                    blockHeader = (BlockHeader *) (temp + readoffset);

                    if (parseLeft < sizeof(BlockHeader) || parseLeft < sizeof(BlockHeader) + blockHeader->length) {
                        leftLength = parseLeft;
                        break;
                    }
                }

                int r = GlobalMetadataManagerPtr->arrangementLookup(blockHeader->fp);
                if(r){
                    ArrangementWriteTask* arrangementWriteTask = new ArrangementWriteTask(
                            (uint8_t*)blockHeader,
                            blockHeader->length + sizeof(BlockHeader),
                            arrangementFilterTask->classId,
                            arrangementFilterTask->classId,
                            arrangementFilterTask->arrangementVersion);
                    GlobalArrangementWritePipelinePtr->addTask(arrangementWriteTask);
                }else{
                    ArrangementWriteTask* arrangementWriteTask = new ArrangementWriteTask(
                            (uint8_t*)blockHeader,
                            blockHeader->length + sizeof(BlockHeader),
                            arrangementFilterTask->classId,
                            arrangementFilterTask->classId + arrangementFilterTask->arrangementVersion,
                            arrangementFilterTask->arrangementVersion);
                    GlobalArrangementWritePipelinePtr->addTask(arrangementWriteTask);
                }
                readoffset += sizeof(BlockHeader) + blockHeader->length;
                parseLeft -= sizeof(BlockHeader) + blockHeader->length;
            }

            delete arrangementFilterTask;
        }
    }

    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<ArrangementFilterTask*> taskList;
    MutexLock mutexLock;
    Condition condition;
};

static ArrangementFilterPipeline* GlobalArrangementFilterPipelinePtr;

#endif //MFDEDUP_ARRANGEMENTFILTERPIPELINE_H
