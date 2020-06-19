//
// Created by Borelset on 2020/5/19.
//
//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_GCPIPIELINE_H
#define MFDEDUP_GCPIPIELINE_H

#include <thread>
#include "../Utility/Likely.h"

DEFINE_uint64(GCReadBufferLength,
4194304, "WriteBufferLength");
DEFINE_uint64(GCWriteBufferLength,
67108864, "WriteBufferLength");

extern std::string LogicFilePath;
extern std::string ClassFilePath;
extern std::string VersionFilePath;


struct BlockHeaderAlter {
    SHA1FP fp;
    uint64_t length;
};

struct VersionFileHeader {
    uint64_t offsetCount;
};

class ClassWriter {
public:
    ClassWriter(FileOperator *fd) : fileOperator(fd) {
        writeBuffer = (uint8_t *) malloc(FLAGS_GCWriteBufferLength);
        writeBufferAvailable = FLAGS_GCWriteBufferLength;
    }

    int writeChunk(BlockHeaderAlter *header, int headerLen, uint8_t *chunk, int chunkLen) {
        if (headerLen + chunkLen > writeBufferAvailable) {
            flush();
        }
        uint8_t *writePoint = writeBuffer + FLAGS_GCWriteBufferLength - writeBufferAvailable;
        memcpy(writePoint, header, headerLen);
        writeBufferAvailable -= headerLen;
        writePoint += headerLen;
        memcpy(writePoint, chunk, chunkLen);
        writeBufferAvailable -= chunkLen;
        return 0;
    }

    ~ClassWriter() {
        flush();
        free(writeBuffer);
    }

private:

    int flush() {
        fileOperator->write(writeBuffer, FLAGS_GCWriteBufferLength - writeBufferAvailable);
        writeBufferAvailable = FLAGS_GCWriteBufferLength;
    }

    uint8_t *writeBuffer;
    int writeBufferAvailable;
    FileOperator *fileOperator;
};

class GCPipeline {
public:
    GCPipeline() : taskAmount(0), runningFlag(true), mutexLock(),
                   condition(mutexLock) {
        worker = new std::thread(std::bind(&GCPipeline::gcCallback, this));
    }

    int addTask(GCTask *gcTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(gcTask);
        taskAmount++;
        condition.notify();
    }

    ~GCPipeline() {
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

private:

    void gcCallback() {
        GCTask *gcTask;

        while (runningFlag) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                gcTask = taskList.front();
                taskList.pop_front();
            }
            uint64_t gcVersion = gcTask->gcVersion;
            uint64_t *offset = (uint64_t *) malloc(sizeof(uint64_t) * gcVersion);
            memset(counter, 0, sizeof(uint64_t) * 4);


            if (likely(gcVersion > 0)) {
                struct timeval t0, t1;
                gettimeofday(&t0, NULL);

                char versionPath[512];
                sprintf(versionPath, VersionFilePath.data(), gcVersion);
                VersionFileHeader versionFileHeader = {
                        .offsetCount = gcVersion
                };

                versionFile = new FileOperator(versionPath, FileOpenType::Write);
                versionFile->write((uint8_t * ) & versionFileHeader, sizeof(VersionFileHeader));
                versionFile->seek(sizeof(VersionFileHeader) + sizeof(uint64_t) * gcVersion);

                uint64_t startClass = (gcVersion - 1) * (gcVersion) / 2 + 1;
                uint64_t endClass = gcVersion * (gcVersion + 1) / 2;
                for (uint64_t i = startClass; i <= endClass; i++) {
                    uint64_t r = gcProcessor(i, gcVersion);
                    offset[i - startClass] = r;
                }

                versionFile->seek(sizeof(VersionFileHeader));
                versionFile->write((uint8_t *) offset, sizeof(uint64_t) * gcVersion);

                versionFile->fdatasync();

                delete versionFile;

                gettimeofday(&t1, NULL);
                uint64_t duration = (t1.tv_sec - t0.tv_sec) * 1000000 + (t1.tv_usec - t0.tv_usec);
                printf("GC finish : version # %lu has been gc, and %lu chunks (%lu bytes) left, %lu chunks (%lu bytes) dropped\n",
                       gcTask->gcVersion, counter[0], counter[1], counter[2], counter[3]);
                printf("Speed : %fMB/s\n", (double) (counter[1] + counter[3]) / duration);
                gcTask->countdownLatch->countDown();
            } else {
                printf("Version 0 does not exist and do not need to GC, skip\n");
                gcTask->countdownLatch->countDown();
            }
        }
    }

    uint64_t gcProcessor(uint64_t classId, uint64_t gcVersion) {
        char pathbuffer_old[512];
        sprintf(pathbuffer_old, ClassFilePath.data(), classId);
        uint64_t migrateChunks = 0;
        uint64_t writeBytes = 0;
        uint64_t dropBytes = 0;
        uint64_t dropChunks = 0;
        uint64_t fileSize = 0;

        struct timeval t0, t1;
        gettimeofday(&t0, NULL);

        {
            ClassWriter classWriter(versionFile);

            FileOperator chunkReader((char *) pathbuffer_old, FileOpenType::Read);
            uint64_t leftLength = FileOperator::size(pathbuffer_old);
            uint8_t *readBuffer = (uint8_t *) malloc(FLAGS_GCReadBufferLength);
            int readBufferLeft = 0;
            int readOffset = 0;
            BlockHeaderAlter *blockHeader;
            uint8_t *chunkPtr;

            while (leftLength > 0) {
                memcpy(readBuffer, readBuffer + readOffset, readBufferLeft);
                int readBytes = chunkReader.read(readBuffer + readBufferLeft,
                                                 FLAGS_GCReadBufferLength - readBufferLeft);
                readBufferLeft += readBytes;
                readOffset = 0;

                while (1) {
                    blockHeader = (BlockHeaderAlter *) (readBuffer + readOffset);
                    chunkPtr = readBuffer + readOffset + sizeof(BlockHeaderAlter);
                    if (readBufferLeft < sizeof(BlockHeaderAlter) ||
                        readBufferLeft < (sizeof(BlockHeaderAlter) + blockHeader->length)) {
                        break;
                    }
                    int r = GlobalMetadataManagerPtr->gcLookup(blockHeader->fp, gcVersion);
                    if (r) {
                        classWriter.writeChunk(blockHeader, sizeof(BlockHeaderAlter), chunkPtr, blockHeader->length);
                        migrateChunks++;
                        writeBytes += blockHeader->length;
                        fileSize += blockHeader->length + sizeof(BlockHeaderAlter);
                    } else {
                        dropBytes += blockHeader->length;
                        dropChunks++;
                    }
                    readOffset += sizeof(BlockHeaderAlter) + blockHeader->length;
                    readBufferLeft -= sizeof(BlockHeaderAlter) + blockHeader->length;
                    leftLength -= sizeof(BlockHeaderAlter) + blockHeader->length;
                }
            }
        }
        gettimeofday(&t1, NULL);
        uint64_t duration = (t1.tv_sec - t0.tv_sec) * 1000000 + (t1.tv_usec - t0.tv_usec);

        remove(pathbuffer_old);

        counter[0] += migrateChunks;
        counter[1] += writeBytes;
        counter[2] += dropChunks;
        counter[3] += dropBytes;

#ifdef DETAILGCINFO
        printf("[GC] : class file %lu has been gc, and %lu chunks left, %lu writeBytes write, %lu chunks dropped, %lu bytes free, speed : %f MB/s\n", classId, migrateChunks, writeBytes, dropChunks, dropBytes, (float)(writeBytes + dropBytes) / duration);
#endif
        return fileSize;
    }

    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<GCTask *> taskList;
    MutexLock mutexLock;
    Condition condition;

    uint64_t counter[4];

    FileOperator *versionFile;
};

GCPipeline *GlobalGCPipelinePtr;

#endif //MFDEDUP_GCPIPIELINE_H
