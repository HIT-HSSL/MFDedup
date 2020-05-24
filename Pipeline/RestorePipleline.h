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
2097152, "WriteBufferLength");

struct RestoreEntry {
    uint64_t pos;
    uint64_t length;
};

class RestorePipeline {
public:
    RestorePipeline() : taskAmount(0), runningFlag(true), mutexLock(),
                        condition(mutexLock) {
        worker = new std::thread(std::bind(&RestorePipeline::writeFileCallback, this));
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
        worker->join();
    }

private:
    void writeFileCallback() {
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
                restoreMap[blockHeader->fp].push_back({pos, blockHeader->length});
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

            FileOperator outputWriter((char *) restoreTask->outputPath.data(), FileOpenType::Write);
            outputWriter.trunc(pos);

            for (auto index : versionList) {
                gettimeofday(&tt0, NULL);
                writeFromVersionFile(index, restoreTask->targetVersion, &outputWriter);
                gettimeofday(&tt1, NULL);
                processEachSource = (tt1.tv_sec - tt0.tv_sec) * 1000000 + (tt1.tv_usec - tt0.tv_usec);
                printf("processVersion # %lu : %lu\n", index, processEachSource);
            }
            for (auto index : classList) {
                gettimeofday(&tt0, NULL);
                writeFromClassFile(index, &outputWriter);
                gettimeofday(&tt1, NULL);
                processEachSource = (tt1.tv_sec - tt0.tv_sec) * 1000000 + (tt1.tv_usec - tt0.tv_usec);
                printf("processClass # %lu : %lu\n", index, processEachSource);
            }

            gettimeofday(&t1, NULL);
            outputWriter.fsync();

            uint64_t restoreDuration = (t1.tv_sec - t0.tv_sec) * 1000000 + (t1.tv_usec - t0.tv_usec);
            float speed = (float) pos / restoreDuration;
            printf("restore done\n");
            printf("Throughput : %f MB/s\n", speed);
            printf("==========================================================\n");

            restoreTask->countdownLatch->countDown();

        }
    }

    uint64_t calculateClassId(uint64_t first, uint64_t second) {
        uint64_t startVersion = first < second ? first : second;
        uint64_t endVersion = first > second ? first : second;
        uint64_t result = (startVersion + 1) * startVersion / 2;
        for (uint64_t i = startVersion + 1; i <= endVersion; i++) {
            result += i - 1;
        }
        return result;
    }

    int writeFromVersionFile(uint64_t versionId, uint64_t restoreVersion, FileOperator *restoreWriter) {
        sprintf(filePath, FLAGS_VersionFilePath.data(), versionId);
        FileOperator versionReader(filePath, FileOpenType::Read);
        int fd = versionReader.getFd();

        VersionFileHeader versionFileHeader;

        //uint64_t r = fread(&versionFileHeader, sizeof(VersionFileHeader), 1, versionReader);
        //.read((uint8_t * ) & versionFileHeader, sizeof(VersionFileHeader));
        read(fd, &versionFileHeader, sizeof(VersionFileHeader));

        uint64_t *offset = (uint64_t *) malloc(versionFileHeader.offsetCount * sizeof(uint64_t));
        //r = fread(offset, versionFileHeader.offsetCount * sizeof(uint64_t), 1, versionReader);
        //versionReader.read((uint8_t *) offset, versionFileHeader.offsetCount * sizeof(uint64_t));
        read(fd, offset, versionFileHeader.offsetCount * sizeof(uint64_t));

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
            memcpy(readBuffer, readBuffer + readOffset, readBufferLeft);
            //uint64_t readBytes = fread(readBuffer + readBufferLeft, 1, FLAGS_RestoreReadBufferLength - readBufferLeft, versionReader);
            uint64_t readBytes = read(fd, readBuffer + readBufferLeft, FLAGS_RestoreReadBufferLength - readBufferLeft);
            //uint64_t readBytes = versionReader.read(readBuffer + readBufferLeft, FLAGS_RestoreReadBufferLength - readBufferLeft);
            readBufferLeft += readBytes;
            readOffset = 0;

            while (1) {
                if (leftLength == 0) break;
                blockHeader = (BlockHeaderAlter * )(readBuffer + readOffset);
                chunkPtr = readBuffer + readOffset + sizeof(BlockHeaderAlter);
                if (readBufferLeft < sizeof(BlockHeaderAlter) ||
                    readBufferLeft < (sizeof(BlockHeaderAlter) + blockHeader->length)) {
                    break;
                }

                auto iter = restoreMap.find(blockHeader->fp);
                assert(iter->second.size() != 0);
                for (auto item : iter->second) {
                    int r = restoreWriter->seek(item.pos);
                    assert(r == 0);
                    restoreWriter->write(chunkPtr, item.length);
                }
                restoreMap.erase(iter);

                readOffset += sizeof(BlockHeaderAlter) + blockHeader->length;
                readBufferLeft -= sizeof(BlockHeaderAlter) + blockHeader->length;
                leftLength -= sizeof(BlockHeaderAlter) + blockHeader->length;
            }
        }

        free(readBuffer);
    }

    int writeFromClassFile(uint64_t classId, FileOperator *restoreWriter) {
        sprintf(filePath, FLAGS_ClassFilePath.data(), classId);
        FileOperator classReader(filePath, FileOpenType::Read);
        uint64_t leftLength = FileOperator::size(filePath);
        uint8_t *readBuffer = (uint8_t *) malloc(FLAGS_RestoreReadBufferLength);
        uint64_t readBufferLeft = 0;
        uint64_t readOffset = 0;
        BlockHeaderAlter *blockHeader;
        uint8_t *chunkPtr;

        while (leftLength > 0) {
            memcpy(readBuffer, readBuffer + readOffset, readBufferLeft);
            uint64_t readBytes = classReader.read(readBuffer + readBufferLeft,
                                             FLAGS_RestoreReadBufferLength - readBufferLeft);
            readBufferLeft += readBytes;
            readOffset = 0;

            while (1) {
                blockHeader = (BlockHeaderAlter * )(readBuffer + readOffset);
                chunkPtr = readBuffer + readOffset + sizeof(BlockHeaderAlter);
                if (readBufferLeft < sizeof(BlockHeaderAlter) ||
                    readBufferLeft < (sizeof(BlockHeaderAlter) + blockHeader->length)) {
                    break;
                }

                auto iter = restoreMap.find(blockHeader->fp);
                assert(iter->second.size() != 0);
                for (auto item : iter->second) {
                    int r = restoreWriter->seek(item.pos);
                    assert(r == 0);
                    restoreWriter->write(chunkPtr, item.length);
                }
                restoreMap.erase(iter);

                readOffset += sizeof(BlockHeaderAlter) + blockHeader->length;
                readBufferLeft -= sizeof(BlockHeaderAlter) + blockHeader->length;
                leftLength -= sizeof(BlockHeaderAlter) + blockHeader->length;
            }
        }
        free(readBuffer);
    }

    char buffer[256];
    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<RestoreTask *> taskList;
    MutexLock mutexLock;
    Condition condition;

    std::unordered_map <SHA1FP, std::list<RestoreEntry>, TupleHasher, TupleEqualer> restoreMap;
    char filePath[256];
};

static RestorePipeline *GlobalRestorePipelinePtr;


#endif //MFDEDUP_RESTOREPIPLELINE_H
