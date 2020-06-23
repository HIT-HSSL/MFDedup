//
// Created by Borelset on 2019/7/29.
//
//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_WRITEFILEPIPELINE_H
#define MFDEDUP_WRITEFILEPIPELINE_H


#include "jemalloc/jemalloc.h"
#include "../MetadataManager/MetadataManager.h"
#include "../Utility/ChunkWriterManager.h"
#include "../Utility/Likely.h"
#include "../Utility/BufferedFileWriter.h"

extern std::string LogicFilePath;

DEFINE_uint64(RecipeFlushBufferSize,
              8388608, "RecipeFlushBufferSize");

class WriteFilePipeline {
public:
    WriteFilePipeline() : runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock),
                          logicFileOperator(nullptr) {
        worker = new std::thread(std::bind(&WriteFilePipeline::writeFileCallback, this));
    }

    int addTask(const WriteTask &writeTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        receiveList.push_back(writeTask);
        taskAmount++;
        condition.notify();
    }

    ~WriteFilePipeline() {
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

    void getStatistics() {
        printf("Write duration:%lu\n", duration);
    }

private:
    void writeFileCallback() {
        struct timeval t0, t1;
        bool newVersionFlag = true;

        BlockHeader blockHeader;
        ChunkWriterManager *chunkWriterManager = nullptr;

        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount = 0;
                condition.notify();
                taskList.swap(receiveList);
            }

            gettimeofday(&t0, NULL);

            if (chunkWriterManager == nullptr) {
                chunkWriterManager = new ChunkWriterManager(TotalVersion);
                duration = 0;
            }

            for (auto &writeTask : taskList) {

                if (!logicFileOperator) {
                    sprintf(buffer, LogicFilePath.c_str(), writeTask.fileID);
                    logicFileOperator = new FileOperator(buffer, FileOpenType::Write);
                    bufferedFileWriter = new BufferedFileWriter(logicFileOperator, FLAGS_RecipeFlushBufferSize);
                }
                blockHeader = {
                        writeTask.sha1Fp,
                        writeTask.bufferLength,
                };
                switch (writeTask.type) {
                    case 0:
                        chunkWriterManager->writeClass((TotalVersion + 1) * TotalVersion / 2,
                                                       (uint8_t * ) & blockHeader, sizeof(BlockHeader),
                                                       writeTask.buffer + writeTask.pos, writeTask.bufferLength);
                        bufferedFileWriter->write((uint8_t * ) & blockHeader, sizeof(BlockHeader));
//                        logicFileOperator->write((uint8_t * ) & blockHeader, sizeof(BlockHeader));
                        break;
                    case 1:
                        bufferedFileWriter->write((uint8_t * ) & blockHeader, sizeof(BlockHeader));
//                        logicFileOperator->write((uint8_t * ) & blockHeader, sizeof(BlockHeader));
                        break;
                    case 2:
                        bufferedFileWriter->write((uint8_t * ) & blockHeader, sizeof(BlockHeader));
//                        chunkWriterManager->writeClass(writeTask.oldClass + TotalVersion - 1,
//                                                       (uint8_t * ) & blockHeader, sizeof(BlockHeader),
//                                                       writeTask.buffer + writeTask.pos, writeTask.bufferLength);
//                        logicFileOperator->write((uint8_t * ) & blockHeader, sizeof(BlockHeader));
                        break;

                }

                if (writeTask.countdownLatch) {
                    printf("WritePipeline finish\n");
                    delete bufferedFileWriter;
                    logicFileOperator->fdatasync();
                    delete logicFileOperator;
                    logicFileOperator = nullptr;
                    writeTask.countdownLatch->countDown();
                    free(writeTask.buffer);

                    delete chunkWriterManager;
                    chunkWriterManager = nullptr;
                }

            }
            taskList.clear();

            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
        }
    }

    FileOperator *logicFileOperator;
    BufferedFileWriter* bufferedFileWriter;
    char buffer[256];
    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list <WriteTask> taskList;
    std::list <WriteTask> receiveList;
    MutexLock mutexLock;
    Condition condition;
    uint64_t duration = 0;

};

static WriteFilePipeline *GlobalWriteFilePipelinePtr;

#endif //MFDEDUP_WRITEFILEPIPELINE_H
