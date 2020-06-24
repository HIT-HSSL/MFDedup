//
// Created by Borelset on 2020/5/18.
//
//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_CHUNKWRITERMANAGER_H
#define MFDEDUP_CHUNKWRITERMANAGER_H

#include "Likely.h"

DEFINE_uint64(WriteBufferLength,
              8388608, "WriteBufferLength");

DEFINE_uint64(ChunkWriterManagerFlushThreshold,
              8, "WriteBufferLength");

extern std::string ClassFilePath;
extern std::string VersionFilePath;

struct WriteBuffer {
    char *buffer;
    uint64_t totalLength;
    uint64_t available;
};


class ChunkWriterManager {
public:
    ChunkWriterManager(uint64_t currentVersion):runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock) {
        startClass = currentVersion * (currentVersion - 1) / 2 + 1;
        endClass = (currentVersion + 1) * currentVersion / 2;

        for (uint64_t i = startClass; i <= endClass; i++) {
            sprintf(pathBuffer, ClassFilePath.data(), i);
            FileOperator *fd = new FileOperator(pathBuffer, FileOpenType::Write);
            fdMap[i] = fd;
            WriteBuffer writeBuffer = {
                    (char *) malloc(FLAGS_WriteBufferLength),
                    FLAGS_WriteBufferLength,
                    FLAGS_WriteBufferLength,
            };
            writeBufferMap[i] = writeBuffer;
        }

        syncWorker = new std::thread(std::bind(&ChunkWriterManager::ChunkWriterManagerCallback, this));
    }

    int writeClass(uint64_t classId, uint8_t *header, uint64_t headerLen, uint8_t *buffer, uint64_t bufferLen) {

        assert(classId >= startClass);
        assert(classId <= endClass);

        auto iter = writeBufferMap.find(classId);
        assert(iter != writeBufferMap.end());

        if ((headerLen + bufferLen) > iter->second.available) {
            classFlush(classId);
        }
        char *writePoint = iter->second.buffer + iter->second.totalLength - iter->second.available;
        memcpy(writePoint, header, headerLen);
        iter->second.available -= headerLen;
        writePoint += headerLen;
        memcpy(writePoint, buffer, bufferLen);
        iter->second.available -= bufferLen;

        return 0;
    }


    ~ChunkWriterManager() {
        addTask(-1);
        syncWorker->join();
        for (auto entry : fdMap) {
            classFlush(entry.first);
        }
        for (auto entry : fdMap){
            entry.second->fdatasync();
            delete entry.second;
        }
        for (auto entry : writeBufferMap) {
            delete entry.second.buffer;
        }
    }

private:
    int classFlush(uint64_t classId) {
        auto fdIter = fdMap.find(classId);
        auto bufferIter = writeBufferMap.find(classId);
        if (fdIter != fdMap.end() && bufferIter != writeBufferMap.end()) {
            uint64_t flushLength = bufferIter->second.totalLength - bufferIter->second.available;
            fdIter->second->write((uint8_t *) bufferIter->second.buffer, flushLength);
            bufferIter->second.available = bufferIter->second.totalLength;
            if(syncCounterMap[classId] >= FLAGS_ChunkWriterManagerFlushThreshold){
                addTask(classId);
                syncCounterMap[classId] = 0;
            }else{
                syncCounterMap[classId]++;
            }
            return 0;
        } else {
            return -1;
        }
    }

    int addTask(uint64_t classId) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(classId);
        taskAmount++;
        condition.notify();
    }

    void ChunkWriterManagerCallback(){
        uint64_t classId;
        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                classId = taskList.front();
                taskList.pop_front();
            }

            if(classId == -1){
                break;
            }

            auto fdIter = fdMap.find(classId);
            fdIter->second->fdatasync();
        }
    }

    std::unordered_map<uint64_t, FileOperator *> fdMap;
    std::unordered_map<uint64_t, WriteBuffer> writeBufferMap;
    std::unordered_map<uint64_t, uint64_t> syncCounterMap;
    uint64_t startClass;
    uint64_t endClass;
    char pathBuffer[1024];

    std::thread* syncWorker;
    bool runningFlag;
    uint64_t taskAmount;
    std::list<uint64_t> taskList;
    MutexLock mutexLock;
    Condition condition;
};

#endif //MFDEDUP_CHUNKWRITERMANAGER_H
