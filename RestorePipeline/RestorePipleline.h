//
// Created by Borelset on 2019/7/31.
//
//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_RESTOREPIPLELINE_H
#define MFDEDUP_RESTOREPIPLELINE_H

#include <bits/types/FILE.h>
#include <stdio.h>
#include "../MetadataManager/MetadataManager.h"

DEFINE_uint64(RestoreReadBufferLength,
16777216, "WriteBufferLength");

struct RestoreEntry {
    uint64_t pos;
};

struct RestoreWritingItem {
    uint8_t *buffer = nullptr;
    uint64_t length = 0;
    SHA1FP sha1Fp;
    bool endflag = false;

    RestoreWritingItem(uint8_t *buf, uint64_t len, const SHA1FP &fp) {
        buffer = (uint8_t *) malloc(len);
        memcpy(buffer, buf, len);
        length = len;
        sha1Fp = fp;
    }

    RestoreWritingItem(bool flag) {
        endflag = true;
    }

    ~RestoreWritingItem() {
        free(buffer);
    }
};


class RestorePipeline {
public:
    RestorePipeline() : taskAmount(0), runningFlag(true), mutexLock(),
                        condition(mutexLock), itemListCondition(itemListLock), itemAmount(0) {
        readWorker = new std::thread(std::bind(&RestorePipeline::scheduleCallback, this));
    }

    int addTask(RestoreTask *restoreTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(restoreTask);
        taskAmount++;
        condition.notify();
    }

    ~RestorePipeline() {
        runningFlag = false;
        condition.notifyAll();
        readWorker->join();
    }

private:
    void scheduleCallback() {
        RestoreTask *restoreTask;

        uint64_t readRecipe = 0, buildMap = 0, calculateSource = 0, processEachSource = 0;

        while (runningFlag) {
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

            printf("==========================================================\n");
            printf("restore start, recipe : %lu\n", restoreTask->targetVersion);

            struct timeval t0, t1, tt0, tt1;
            gettimeofday(&t0, NULL);

            BlockHeader *blockHeader;
            FileOperator recipeFD((char *) restoreTask->path.data(), FileOpenType::Read);
            uint64_t size = FileOperator::size(restoreTask->path);
            uint8_t *recipeBuffer = (uint8_t *) malloc(size);
            recipeFD.read(recipeBuffer, size);
            uint64_t count = size / sizeof(BlockHeader);
            assert(count * sizeof(BlockHeader) == size);
            gettimeofday(&tt0, NULL);
            readRecipe = (tt0.tv_sec - t0.tv_sec) * 1000000 + (tt0.tv_usec - t0.tv_usec);
            printf("readRecipe:%lu\n", readRecipe);

            uint64_t pos = 0;
            for (uint64_t i = 0; i < count; i++) {
                blockHeader = (BlockHeader * )(recipeBuffer + i * sizeof(BlockHeader));
                restoreMap[blockHeader->fp].push_back({pos});
                pos += blockHeader->length;
            }
            gettimeofday(&tt1, NULL);
            buildMap = (tt1.tv_sec - tt0.tv_sec) * 1000000 + (tt1.tv_usec - tt0.tv_usec);
            printf("buildMap:%lu\n", buildMap);
            printf("total length :%lu\n", pos);

            std::vector <uint64_t> classList, versionList;
            for (uint64_t i = restoreTask->targetVersion; i <= restoreTask->maxVersion - 1; i++) {
                versionList.push_back(i);
                printf("version # %lu is required\n", i);
            }
            uint64_t baseClass = (restoreTask->maxVersion - 1) * restoreTask->maxVersion / 2 + 1;
            for (uint64_t i = baseClass; i < baseClass + restoreTask->targetVersion; i++) {
                classList.push_back(i);
                printf("class # %lu is required\n", i);
            }
            gettimeofday(&tt0, NULL);
            calculateSource = (tt0.tv_sec - tt1.tv_sec) * 1000000 + (tt0.tv_usec - tt1.tv_usec);
            printf("calculateSource:%lu\n", calculateSource);

            CountdownLatch finishCountdown(1);
            writeWorker = new std::thread(std::bind(&RestorePipeline::writeFileCallback, this, pos, &finishCountdown,
                                                    restoreTask->outputPath));

            for (auto index : versionList) {
                gettimeofday(&tt0, NULL);
                readFromVersionFile(index, restoreTask->targetVersion);
                gettimeofday(&tt1, NULL);
                processEachSource = (tt1.tv_sec - tt0.tv_sec) * 1000000 + (tt1.tv_usec - tt0.tv_usec);
                printf("processVersion # %lu : %lu\n", index, processEachSource);
            }
            for (auto index : classList) {
                gettimeofday(&tt0, NULL);
                readFromClassFile(index);
                gettimeofday(&tt1, NULL);
                processEachSource = (tt1.tv_sec - tt0.tv_sec) * 1000000 + (tt1.tv_usec - tt0.tv_usec);
                printf("processClass # %lu : %lu\n", index, processEachSource);
            }
            itemReceiveList.push_back(new RestoreWritingItem(true));

            finishCountdown.wait();
            gettimeofday(&t1, NULL);

            uint64_t restoreDuration = (t1.tv_sec - t0.tv_sec) * 1000000 + (t1.tv_usec - t0.tv_usec);
            float speed = (float) pos / restoreDuration;
            printf("restore done\n");
            printf("Total duration:%lu\n", restoreDuration);
            printf("Throughput : %f MB/s\n", speed);
            printf("==========================================================\n");

            restoreTask->countdownLatch->countDown();

        }
    }

    void writeFileCallback(uint64_t totalLength, CountdownLatch *countdownLatch, const std::string &path) {
        FileOperator outputWriter((char *) path.data(), FileOpenType::Write);
        outputWriter.trunc(totalLength);
        int fd = outputWriter.getFd();
        uint64_t writeDuration = 0, waitDuration = 0;

        struct timeval t0, t1, wt0, wt1;

        while (runningFlag) {
            gettimeofday(&wt0, NULL);
            {
                MutexLockGuard mutexLockGuard(itemListLock);
                while (!itemAmount) {
                    itemListCondition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                itemAmount = 0;
                itemProcessList.swap(itemReceiveList);
            }
            gettimeofday(&wt1, NULL);
            waitDuration += (wt1.tv_sec - wt0.tv_sec) * 1000000 + (wt1.tv_usec - wt0.tv_usec);

            gettimeofday(&t0, NULL);
            for (auto item : itemProcessList) {
                if (unlikely(item->endflag)) {
                    delete item;
                    goto writeFileEnd;
                }
                auto iter = restoreMap.find(item->sha1Fp);
                assert(iter->second.size() != 0);
                for (auto i : iter->second) {
                    uint64_t r = lseek64(fd, i.pos, SEEK_SET);
                    assert(r == i.pos);
                    write(fd, item->buffer, item->length);
                }
                delete item;
            }
            gettimeofday(&t1, NULL);
            writeDuration += (t1.tv_sec - t0.tv_sec) * 1000000 + (t1.tv_usec - t0.tv_usec);
            itemProcessList.clear();

        }
        writeFileEnd:
        outputWriter.fsync();
        printf("write duration : %lu, wait duration : %lu, readdd : %lu\n", writeDuration, waitDuration, readdd);
        countdownLatch->countDown();
    }

    int readFromVersionFile(uint64_t versionId, uint64_t restoreVersion) {
        sprintf(filePath, FLAGS_VersionFilePath.data(), versionId);
        FileOperator versionReader(filePath, FileOpenType::Read);
        int versionFileFD = versionReader.getFd();

        VersionFileHeader versionFileHeader;

        read(versionFileFD, &versionFileHeader, sizeof(VersionFileHeader));

        uint64_t *offset = (uint64_t *) malloc(versionFileHeader.offsetCount * sizeof(uint64_t));
        read(versionFileFD, offset, versionFileHeader.offsetCount * sizeof(uint64_t));

        uint64_t leftLength = 0;
        for (int i = 0; i < restoreVersion; i++) {
            leftLength += offset[i];
        }

        uint8_t *readBuffer = (uint8_t *) malloc(FLAGS_RestoreReadBufferLength);
        uint64_t readBufferLeft = 0;
        uint64_t readOffset = 0;
        BlockHeaderAlter *blockHeader;
        uint8_t *chunkPtr;

        while (leftLength > 0) {
            uint64_t bytesToRead =
                    leftLength > FLAGS_RestoreReadBufferLength ? FLAGS_RestoreReadBufferLength : leftLength;
            memcpy(readBuffer, readBuffer + readOffset, readBufferLeft);

            gettimeofday(&rt0, NULL);
            uint64_t bytesFinallyRead = read(versionFileFD, readBuffer + readBufferLeft, bytesToRead - readBufferLeft);
            gettimeofday(&rt1, NULL);
            readdd += (rt1.tv_sec - rt0.tv_sec) * 1000000 + (rt1.tv_usec - rt0.tv_usec);

            readBufferLeft += bytesFinallyRead;
            readOffset = 0;

            while (1) {


                if (leftLength == 0) break;
                blockHeader = (BlockHeaderAlter * )(readBuffer + readOffset);
                chunkPtr = readBuffer + readOffset + sizeof(BlockHeaderAlter);
                if (readBufferLeft < sizeof(BlockHeaderAlter) ||
                    readBufferLeft < (sizeof(BlockHeaderAlter) + blockHeader->length)) {
                    break;
                }

                RestoreWritingItem *restoreWritingItem = new RestoreWritingItem(chunkPtr, blockHeader->length,
                                                                                blockHeader->fp);
                {
                    MutexLockGuard mutexLockGuard(itemListLock);
                    itemReceiveList.push_back(restoreWritingItem);
                    itemAmount++;
                    itemListCondition.notifyAll();
                }

                readOffset += sizeof(BlockHeaderAlter) + blockHeader->length;
                readBufferLeft -= sizeof(BlockHeaderAlter) + blockHeader->length;
                leftLength -= sizeof(BlockHeaderAlter) + blockHeader->length;
            }
        }

        free(readBuffer);
    }

    int readFromClassFile(uint64_t classId) {
        sprintf(filePath, FLAGS_ClassFilePath.data(), classId);
        FileOperator classReader(filePath, FileOpenType::Read);
        int fd = classReader.getFd();

        uint64_t leftLength = FileOperator::size(filePath);
        uint8_t *readBuffer = (uint8_t *) malloc(FLAGS_RestoreReadBufferLength);
        uint64_t readBufferLeft = 0;
        uint64_t readOffset = 0;
        BlockHeaderAlter *blockHeader;
        uint8_t *chunkPtr;


        while (leftLength > 0) {
            uint64_t bytesToRead =
                    leftLength > FLAGS_RestoreReadBufferLength ? FLAGS_RestoreReadBufferLength : leftLength;
            memcpy(readBuffer, readBuffer + readOffset, readBufferLeft);

            gettimeofday(&rt0, NULL);
            uint64_t bytesFinallyRead = read(fd, readBuffer + readBufferLeft, bytesToRead - readBufferLeft);
            gettimeofday(&rt1, NULL);
            readdd += (rt1.tv_sec - rt0.tv_sec) * 1000000 + (rt1.tv_usec - rt0.tv_usec);

            readBufferLeft += bytesFinallyRead;
            readOffset = 0;
            while (1) {
                blockHeader = (BlockHeaderAlter * )(readBuffer + readOffset);
                chunkPtr = readBuffer + readOffset + sizeof(BlockHeaderAlter);
                if (readBufferLeft < sizeof(BlockHeaderAlter) ||
                    readBufferLeft < (sizeof(BlockHeaderAlter) + blockHeader->length)) {
                    break;
                }

//                auto iter = restoreMap.find(blockHeader->fp);
//                assert(iter->second.size() != 0);
                RestoreWritingItem *restoreWritingItem = new RestoreWritingItem(chunkPtr, blockHeader->length,
                                                                                blockHeader->fp);
                {
                    MutexLockGuard mutexLockGuard(itemListLock);
                    itemReceiveList.push_back(restoreWritingItem);
                    itemAmount++;
                    itemListCondition.notifyAll();
                }


                readOffset += sizeof(BlockHeaderAlter) + blockHeader->length;
                readBufferLeft -= sizeof(BlockHeaderAlter) + blockHeader->length;
                leftLength -= sizeof(BlockHeaderAlter) + blockHeader->length;
            }
        }
        free(readBuffer);
    }


    char buffer[256];
    bool runningFlag;
    std::thread *readWorker;
    std::thread *writeWorker;
    uint64_t taskAmount;
    std::list<RestoreTask *> taskList;
    MutexLock mutexLock;
    Condition condition;

    std::list<RestoreWritingItem *> itemReceiveList;
    std::list<RestoreWritingItem *> itemProcessList;
    MutexLock itemListLock;
    Condition itemListCondition;
    uint64_t itemAmount;

    struct timeval rt0, rt1;
    uint64_t readdd = 0;

    std::unordered_map<SHA1FP, std::list<RestoreEntry>, TupleHasher, TupleEqualer> restoreMap;
    char filePath[256];
};

static RestorePipeline *GlobalRestorePipelinePtr;


#endif //MFDEDUP_RESTOREPIPLELINE_H
