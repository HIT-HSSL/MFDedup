//  Copyright (c) Xiangyu Zou, 2020. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_HASHINGPIPELINE_H
#define MFDEDUP_HASHINGPIPELINE_H


#include "jemalloc/jemalloc.h"
#include "isa-l_crypto/mh_sha1.h"
#include "openssl/sha.h"
#include "DeduplicationPipeline.h"
#include <assert.h>

class HashingPipeline {
public:
    HashingPipeline() : runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock) {
        worker = new std::thread(std::bind(&HashingPipeline::hashingWorkerCallback, this));
    }

    int addTask(const DedupTask &dedupTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        receiceList.push_back(dedupTask);
        taskAmount++;
        condition.notifyAll();

    }

    ~HashingPipeline() {

        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

    void getStatistics() {
        printf("Hashing Duration : %lu\n", duration);
    }

private:
    void hashingWorkerCallback() {
        mh_sha1_ctx ctx;
        //SHA_CTX ctx;
        struct timeval t0, t1, t2;
        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount = 0;
                taskList.swap(receiceList);
            }

            if(unlikely(newVersion)){
                duration = 0;
                newVersion = false;
            }

            gettimeofday(&t0, NULL);
            for (auto &dedupTask : taskList) {
                //openssl sha1
                //SHA1_Init(&ctx);
                //SHA1_Update(&ctx, dedupTask.buffer + dedupTask.pos, (uint32_t) dedupTask.length);
                //SHA1_Final((unsigned char *) &dedupTask.fp, &ctx);

                //isa sha1

                mh_sha1_init(&ctx);
                mh_sha1_update_avx2(&ctx, dedupTask.buffer + dedupTask.pos, (uint32_t) dedupTask.length);
                mh_sha1_finalize_avx2(&ctx, &dedupTask.fp);

                if (dedupTask.countdownLatch) {
                    printf("HashingPipeline finish\n");
                    dedupTask.countdownLatch->countDown();
                    newVersion = true;
                }
                GlobalDeduplicationPipelinePtr->addTask(dedupTask);
            }
            taskList.clear();
            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;

        }
    }

    std::thread *worker;
    std::list <DedupTask> taskList;
    std::list <DedupTask> receiceList;
    int taskAmount;
    bool runningFlag;
    MutexLock mutexLock;
    Condition condition;
    uint64_t duration = 0;

    bool newVersion = true;
};

static HashingPipeline *GlobalHashingPipelinePtr;


#endif //MFDEDUP_HASHINGPIPELINE_H
