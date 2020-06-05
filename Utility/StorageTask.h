//
// Created by BorelsetR on 2019/7/23.
//

#ifndef MDFDEDUP_STORAGETASK_H
#define MDFDEDUP_STORAGETASK_H

#include "Lock.h"
#include <list>
#include <tuple>
#include <cstring>

struct SHA1FP {
    //std::tuple<uint32_t, uint32_t, uint32_t, uint32_t, uint32_t> fp;
    uint64_t fp1;
    uint32_t fp2, fp3, fp4;

    void print() {
        printf("%lu:%d:%d:%d\n", fp1, fp2, fp3, fp4);
    }
};


struct DedupTask {
    uint8_t *buffer;
    uint64_t pos;
    uint64_t length;
    SHA1FP fp;
    uint64_t fileID;
    CountdownLatch *countdownLatch = nullptr;
    uint64_t index;
};

struct WriteTask {
    int type;
    uint8_t *buffer;
    uint64_t pos;
    uint64_t bufferLength;
    uint64_t oldClass;
    uint64_t fileID;
    SHA1FP sha1Fp;
    CountdownLatch *countdownLatch = nullptr;
    uint64_t index;

};

struct ChunkTask {
    uint8_t *buffer = nullptr;
    uint64_t length;
    uint64_t fileID;
    uint64_t end;
    CountdownLatch *countdownLatch = nullptr;
    uint64_t index;
};

struct StorageTask {
    std::string path;
    uint8_t *buffer = nullptr;
    uint64_t length;
    uint64_t fileID;
    uint64_t end;
    CountdownLatch *countdownLatch = nullptr;

    void destruction() {
        if (buffer) free(buffer);
    }
};

struct RestoreTask {
    uint64_t maxVersion;
    uint64_t targetVersion;
};

struct RestoreParseTask {
    uint8_t *buffer = nullptr;
    uint64_t length;
    bool endFlag = false;
    uint64_t index = 0;
    uint64_t beginPos = 0;

    RestoreParseTask(uint8_t *buf, uint64_t len) {
        buffer = buf;
        length = len;
        beginPos = 0;
    }

    RestoreParseTask(bool flag) {
        endFlag = true;
    }

    ~RestoreParseTask() {
        if (buffer) {
            free(buffer);
        }
    }
};

struct RestoreWriteTask {
    uint8_t *buffer = nullptr;
    uint64_t pos;
    uint64_t length;
    bool endFlag = false;

    RestoreWriteTask(uint8_t *buf, uint64_t p, uint64_t len) {
        buffer = (uint8_t *) malloc(len);
        memcpy(buffer, buf, len);
        length = len;
        pos = p;
    }

    RestoreWriteTask(bool flag) {
        endFlag = true;
    }

    ~RestoreWriteTask() {
        if (buffer) {
            free(buffer);
        }
    }
};

struct GCTask {
    uint64_t gcVersion;
    CountdownLatch *countdownLatch = nullptr;
};

#endif //MDFDEDUP_STORAGETASK_H
