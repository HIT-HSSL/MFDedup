//
// Created by Borelset on 2020/5/18.
//
//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_CHUNKWRITERMANAGER_H
#define MFDEDUP_CHUNKWRITERMANAGER_H

DEFINE_uint64(WriteBufferLength,
              8388608, "WriteBufferLength");

extern std::string ClassFilePath;
extern std::string VersionFilePath;

struct WriteBuffer {
    char *buffer;
    uint64_t totalLength;
    uint64_t available;
};


class ChunkWriterManager {
public:
    ChunkWriterManager(uint64_t currentVersion) {
        startClass = currentVersion * (currentVersion - 1) / 2 + 1;
        endClass = (currentVersion + 1) * currentVersion / 2;

        for (uint64_t i = endClass/*startClass*/; i <= endClass; i++) {
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
        for (auto entry : fdMap) {
            classFlush(entry.first);
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
            return 0;
        } else {
            return -1;
        }
    }

    uint64_t currentVersion;
    std::unordered_map<uint64_t, FileOperator *> fdMap;
    std::unordered_map <uint64_t, WriteBuffer> writeBufferMap;
    uint64_t startClass;
    uint64_t endClass;
    char pathBuffer[1024];
};

#endif //MFDEDUP_CHUNKWRITERMANAGER_H
