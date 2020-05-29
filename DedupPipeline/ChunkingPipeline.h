//
// Created by Borelset on 2019/7/29.
//

//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_CHUNKINGPIPELINE_H
#define MFDEDUP_CHUNKINGPIPELINE_H

#include <sys/time.h>
#include "../RollHash/Gear.h"
#include "../RollHash/Rabin.h"
#include "gflags/gflags.h"
#include <thread>
#include "isa-l_crypto/mh_sha1.h"
#include "openssl/sha.h"
#include "HashingPipeline.h"
#include "../RollHash/rabin_chunking.h"

DEFINE_string(ChunkingMethod,
"Gear", "chunking method in chunking");
DEFINE_int32(ExceptSize,
8192, "average chunk size");

class ChunkingPipeline {
public:
    ChunkingPipeline()
            : taskAmount(0),
              runningFlag(true),
              mutexLock(),
              condition(mutexLock) {
        if (FLAGS_ChunkingMethod == std::string("Gear")) {
            rollHash = new Gear();
            matrix = rollHash->getMatrix();
            worker = new std::thread(std::bind(&ChunkingPipeline::chunkingWorkerCallbackGear, this));
        } else if (FLAGS_ChunkingMethod == std::string("Rabin")) {
            worker = new std::thread(std::bind(&ChunkingPipeline::chunkingWorkerCallbackRabin, this));
        } else if (FLAGS_ChunkingMethod == std::string("Fixed")) {
            worker = new std::thread(std::bind(&ChunkingPipeline::chunkingWorkerCallbackFixed, this));
        }
        MaxChunkSize = FLAGS_ExceptSize * 8;
        MinChunkSize = FLAGS_ExceptSize / 4;

        printf("ChunkingPipeline inited, Max chunk size=%d, Min chunk size=%d\n", MaxChunkSize, MinChunkSize);
    }

    int addTask(ChunkTask chunkTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(chunkTask);
        taskAmount++;
        condition.notify();
    }

    void getStatistics() {
        printf("Chunking Duration:%lu\n", duration);
    }

    ~ChunkingPipeline() {
        delete rollHash;
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

private:

    void chunkingWorkerCallbackGear() {
        mh_sha1_ctx ctx;
        //SHA_CTX ctx;
        uint64_t posPtr = 0;
        uint64_t base = 0;

        if (FLAGS_ExceptSize == 8192) {
            chunkMask = 0x0000d90f03530000;//32
            chunkMask2 = 0x0000d90003530000;//2
        } else if (FLAGS_ExceptSize == 4096) {
            chunkMask = 0x0000d90703530000;//16
            chunkMask2 = 0x0000590003530000;//1
        } else if (FLAGS_ExceptSize == 16384) {
            chunkMask = 0x0000d90f13530000;//64
            chunkMask2 = 0x0000d90103530000;//4
        }

        uint64_t counter = 0;
        uint8_t *data = nullptr;
        DedupTask dedupTask;
        CountdownLatch *cd;
        bool newFileFlag = true;
        std::list <DedupTask> saveList;
        ChunkTask chunkTask;
        bool flag = false;

        struct timeval t1, t0;

        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                chunkTask = taskList.front();
                taskList.pop_front();
            }

            gettimeofday(&t0, NULL);

            if (newFileFlag) {
                posPtr = 0;
                base = 0;
                data = chunkTask.buffer;
                newFileFlag = false;
                flag = false;
            }
            uint64_t end = chunkTask.end;

            dedupTask.buffer = chunkTask.buffer;
            dedupTask.length = chunkTask.length;
            dedupTask.fileID = chunkTask.fileID;

            if (!chunkTask.countdownLatch) {
                while (end - posPtr > MaxChunkSize) {
                    int chunkSize = fastcdc_chunk_data(data + posPtr, end - posPtr);
                    dedupTask.pos = base;
                    dedupTask.length = chunkSize;
                    dedupTask.index++;
                    GlobalHashingPipelinePtr->addTask(dedupTask);
                    base += chunkSize;
                    posPtr += chunkSize;
                }
            } else {
                while (end != posPtr) {
                    int chunkSize = fastcdc_chunk_data(data + posPtr, end - posPtr);
                    dedupTask.pos = base;
                    dedupTask.length = chunkSize;
                    dedupTask.index++;
                    if (end == posPtr + chunkSize) {
                        dedupTask.countdownLatch = chunkTask.countdownLatch;
                        flag = true;
                    }
                    GlobalHashingPipelinePtr->addTask(dedupTask);
                    base += chunkSize;
                    posPtr += chunkSize;
                }
            }
            if (flag) {
                chunkTask.countdownLatch->countDown();
                printf("ChunkingPipeline finish\n");
                newFileFlag = true;
                dedupTask.countdownLatch = nullptr;
            }


            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
        }
    }

    void chunkingWorkerCallbackRabin() {
        mh_sha1_ctx ctx;
        //SHA_CTX ctx;
        uint64_t posPtr = 0;
        uint64_t base = 0;
        uint64_t fp = 0;
        const uint64_t chunkMask = 0x0000d90f03530000;
        const uint64_t chunkMask2 = 0x0000d90003530000;
        uint64_t rabinMask = FLAGS_ExceptSize - 1;
        uint64_t counter = 0;
        uint8_t *data = nullptr;
        DedupTask dedupTask;
        CountdownLatch *cd;
        bool newFileFlag = true;
        std::list <DedupTask> saveList;
        ChunkTask chunkTask;
        uint64_t s = 0, e = 0, cs = 0, n = 0;

        struct timeval t1, t0;
        struct timeval lt0, lt1;
        struct timeval ct0, ct1, ct2, ct3, ct4, ct5, ct6;

        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (!runningFlag) break;
                }
                if (!runningFlag) continue;
                taskAmount--;
                chunkTask = taskList.front();
                taskList.pop_front();
            }

            gettimeofday(&t0, NULL);

            if (newFileFlag) {
                posPtr = 0;
                base = 0;
                fp = 0;
                data = chunkTask.buffer;
                newFileFlag = false;
            }
            uint64_t end = chunkTask.end;

            dedupTask.buffer = chunkTask.buffer;
            dedupTask.length = chunkTask.length;
            dedupTask.fileID = chunkTask.fileID;

            if (!chunkTask.countdownLatch) {

                while (end - posPtr > MaxChunkSize) {

                    fp = rollHashRabin.rolling(data + posPtr);

                    if ((fp & rabinMask) == 0x78) {

                        dedupTask.pos = base;
                        dedupTask.length = posPtr - base + 1;
                        dedupTask.index = order++;
                        cs += posPtr - base + 1;
                        n++;


                        GlobalHashingPipelinePtr->addTask(dedupTask);

                        base = posPtr + 1;
                        posPtr += MinChunkSize;

                    }
                    posPtr++;

                }
            } else {
                while (posPtr < end) {
                    fp = rollHashRabin.rolling(data + posPtr);
                    if ((fp & rabinMask) == 0x78) {
                        dedupTask.pos = base;
                        dedupTask.length = posPtr - base + 1;
                        dedupTask.index = order++;
                        cs += posPtr - base + 1;
                        n++;

                        GlobalHashingPipelinePtr->addTask(dedupTask);

                        base = posPtr + 1;
                        posPtr += MinChunkSize;

                    }
                    posPtr++;
                }
                if (base != posPtr) {
                    dedupTask.pos = base;
                    dedupTask.length = end - base;
                    dedupTask.index = order++;

                    dedupTask.countdownLatch = chunkTask.countdownLatch;

                    GlobalHashingPipelinePtr->addTask(dedupTask);
                }
                chunkTask.countdownLatch->countDown();
                newFileFlag = true;
                dedupTask.countdownLatch = nullptr;
            }

            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec - t0.tv_sec) * 1000000 + t1.tv_usec - t0.tv_usec;
        }

    }

    int fastcdc_chunk_data(unsigned char *p, uint64_t n) {

        uint64_t fingerprint = 0, digest;
        int i = MinChunkSize, Mid = MinChunkSize + 8 * 1024;
        //return n;

        if (n <= MinChunkSize) //the minimal  subChunk Size.
            return n;
        //windows_reset();
        if (n > MaxChunkSize)
            n = MaxChunkSize;
        else if (n < Mid)
            Mid = n;
        while (i < Mid) {
            fingerprint = (fingerprint << 1) + (matrix[p[i]]);
            if ((!(fingerprint & chunkMask))) { //AVERAGE*2, *4, *8
                return i;
            }
            i++;
        }
        while (i < n) {
            fingerprint = (fingerprint << 1) + (matrix[p[i]]);
            if ((!(fingerprint & chunkMask2))) { //Average/2, /4, /8
                return i;
            }
            i++;
        }
        return i;
    }

    void chunkingWorkerCallbackFixed() {
        mh_sha1_ctx ctx;
        //SHA_CTX ctx;
        uint64_t posPtr = 0;
        uint64_t base = 0;

        uint64_t rabinMask = 8191;
        uint64_t counter = 0;
        uint8_t *data = nullptr;
        DedupTask dedupTask;
        CountdownLatch *cd;
        bool newFileFlag = true;
        std::list <DedupTask> saveList;
        ChunkTask chunkTask;
        bool flag = false;

        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (!runningFlag) break;
                }
                if (!runningFlag) continue;
                taskAmount--;
                chunkTask = taskList.front();
                taskList.pop_front();
            }

            if (newFileFlag) {
                posPtr = 0;
                base = 0;
                data = chunkTask.buffer;
                newFileFlag = false;
                flag = false;
            }
            uint64_t end = chunkTask.end;

            dedupTask.buffer = chunkTask.buffer;
            dedupTask.length = chunkTask.length;
            dedupTask.fileID = chunkTask.fileID;

            if (!chunkTask.countdownLatch) {
                while (end - posPtr > MaxChunkSize) {
                    int chunkSize = fix_chunk_data(data + posPtr, end - posPtr);
                    dedupTask.pos = base;
                    dedupTask.length = chunkSize;
                    dedupTask.index++;
                    GlobalHashingPipelinePtr->addTask(dedupTask);
                    base += chunkSize;
                    posPtr += chunkSize;
                }
            } else {
                while (end != posPtr) {
                    int chunkSize = fix_chunk_data_end(data + posPtr, end - posPtr);
                    dedupTask.pos = base;
                    dedupTask.length = chunkSize;
                    dedupTask.index++;
                    if (end == posPtr + chunkSize) {
                        dedupTask.countdownLatch = chunkTask.countdownLatch;
                        flag = true;
                    }
                    GlobalHashingPipelinePtr->addTask(dedupTask);
                    base += chunkSize;
                    posPtr += chunkSize;
                }
            }
            if (flag) {
                chunkTask.countdownLatch->countDown();
                newFileFlag = true;
                dedupTask.countdownLatch = nullptr;
            }

        }
    }

    int fix_chunk_data(unsigned char *p, uint64_t n) {
        return FLAGS_ExceptSize;
    }

    int fix_chunk_data_end(unsigned char *p, uint64_t n) {
        if (n < FLAGS_ExceptSize) {
            return n;
        } else {
            return FLAGS_ExceptSize;
        }
    }

    RollHash *rollHash;
    Rabin rollHashRabin;
    std::thread *worker;
    std::list <ChunkTask> taskList;
    int taskAmount;
    bool runningFlag;
    MutexLock mutexLock;
    Condition condition;
    uint64_t *matrix;
    uint64_t duration = 0;
    uint64_t order = 0;
    uint64_t chunkMask;
    uint64_t chunkMask2;

    int MaxChunkSize;
    int MinChunkSize;
};

static ChunkingPipeline *GlobalChunkingPipelinePtr;

#endif //MFDEDUP_CHUNKINGPIPELINE_H
