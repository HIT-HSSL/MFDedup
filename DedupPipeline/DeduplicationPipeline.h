//
// Created by Borelset on 2019/7/29.
//

//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_DEDUPLICATIONPIPELINE_H
#define MFDEDUP_DEDUPLICATIONPIPELINE_H


#include "jemalloc/jemalloc.h"
#include "../MetadataManager/MetadataManager.h"
#include "WriteFilePipeline.h"
#include <assert.h>
#include "../Utility/Likely.h"

class DeduplicationPipeline {
public:
    DeduplicationPipeline()
            : taskAmount(0),
              runningFlag(true),
              mutexLock(),
              condition(mutexLock) {
        worker = new std::thread(std::bind(&DeduplicationPipeline::deduplicationWorkerCallback, this));

    }

    int addTask(const DedupTask &dedupTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        receiceList.push_back(dedupTask);
        taskAmount++;
        condition.notifyAll();
    }

    ~DeduplicationPipeline() {
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

    void getStatistics() {
        printf("Deduplicating Duration : %lu\n", duration);
        printf("new:%lu, iv:%lu, nv:%lu, it:%lu\n", chunkCounter[0], chunkCounter[1], chunkCounter[2], chunkCounter[3]);
        printf("Total Length : %lu, Unique Length : %lu, Adjacent duplicates : %lu, Dedup Ratio : %f\n", totalLength, afterDedupLength, adjacentDuplicates,
               (float) totalLength / afterDedupLength);
    }


private:
    void deduplicationWorkerCallback() {
        WriteTask writeTask;

        struct timeval t0, t1;
        std::list <WriteTask> saveList;
        uint8_t *currentTask;
        bool newVersionFlag = true;

        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                //printf("get task\n");
                taskAmount = 0;
                taskList.swap(receiceList);
            }

            if (newVersionFlag) {
                for (int i = 0; i < 4; i++) {
                    chunkCounter[i] = 0;
                }
                newVersionFlag = false;
                duration = 0;
            }

            for (const auto &dedupTask : taskList) {

                gettimeofday(&t0, NULL);

                writeTask.fileID = dedupTask.fileID;
                writeTask.index = dedupTask.index;

                uint64_t oldClass;
                LookupResult lookupResult = GlobalMetadataManagerPtr->dedupLookup(dedupTask.fp, dedupTask.length);
                chunkCounter[(int) lookupResult]++;

                writeTask.type = (int) lookupResult;
                writeTask.buffer = dedupTask.buffer;
                writeTask.pos = dedupTask.pos;
                writeTask.bufferLength = dedupTask.length;
                writeTask.sha1Fp = dedupTask.fp;
                writeTask.oldClass = oldClass;

                totalLength += dedupTask.length;

                switch (lookupResult) {
                    case LookupResult::Unique:
                        GlobalMetadataManagerPtr->newChunkAddRecord(writeTask.sha1Fp);
                        afterDedupLength += dedupTask.length;
                        break;
                    case LookupResult::InternalDedup:
                        break;
                    case LookupResult::AdjacentDedup:
                        adjacentDuplicates += dedupTask.length;
                        GlobalMetadataManagerPtr->neighborAddRecord(writeTask.sha1Fp);
                        break;
                }

                gettimeofday(&t1, NULL);
                duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;

                if (unlikely(dedupTask.countdownLatch)) {
                    printf("DedupPipeline finish\n");
                    writeTask.countdownLatch = dedupTask.countdownLatch;
                    dedupTask.countdownLatch->countDown();
                    //GlobalMetadataManagerPtr->tableRolling();
                    newVersionFlag = true;

                    GlobalWriteFilePipelinePtr->addTask(writeTask);
                } else {

                    GlobalWriteFilePipelinePtr->addTask(writeTask);
                }

                writeTask.countdownLatch = nullptr;
            }
            taskList.clear();
        }

    }

    std::thread *worker;
    std::list <DedupTask> taskList;
    std::list <DedupTask> receiceList;
    int taskAmount;
    bool runningFlag;
    MutexLock mutexLock;
    Condition condition;


    uint64_t totalLength = 0;
    uint64_t afterDedupLength = 0;
    uint64_t adjacentDuplicates = 0;

    uint64_t chunkCounter[4] = {0, 0, 0, 0};

    uint64_t duration = 0;

};

static DeduplicationPipeline *GlobalDeduplicationPipelinePtr;

#endif //MFDEDUP_DEDUPLICATIONPIPELINE_H
