//
// Created by Borelset on 2019/7/29.
//

//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_MATADATAMANAGER_H
#define MFDEDUP_MATADATAMANAGER_H

#include <map>
#include "../Utility/StorageTask.h"
#include <unordered_map>

uint64_t shadMask = 0x7;

int ReplaceThreshold = 10;

struct TupleHasher {
    std::size_t
    operator()(const SHA1FP &key) const {
        return key.fp1;
    }
};

struct TupleEqualer {
    bool operator()(const SHA1FP &lhs, const SHA1FP &rhs) const {
        return lhs.fp1 == rhs.fp1 && lhs.fp2 == rhs.fp2 && lhs.fp3 == rhs.fp3 && lhs.fp4 == rhs.fp4;
    }
};

enum class LookupResult {
    New,
    InnerDedup,
    NeighborDedup,
    IntervalDedup,
};

struct MetaEntry {
    uint64_t length;
    uint64_t classId;
};

class MetadataManager {
public:
    MetadataManager() {

    }

    LookupResult dedupLookup(const SHA1FP &sha1Fp, uint64_t currentVersion, uint64_t *oc) {
        MutexLockGuard mutexLockGuard(tableLock);
        std::unordered_map <SHA1FP, MetaEntry, TupleHasher, TupleEqualer> &currentTable = metaTable[currentVersion];

        auto innerDedupIter = currentTable.find(sha1Fp);
        if (innerDedupIter != currentTable.end()) {
            return LookupResult::InnerDedup;
        }

        auto preTableIter = metaTable.find(currentVersion - 1);
        if (preTableIter == metaTable.end()) {
            assert(currentVersion == 1);
            return LookupResult::New;
        }
        std::unordered_map <SHA1FP, MetaEntry, TupleHasher, TupleEqualer> &preTable = preTableIter->second;

        auto neighborDedupIter = preTable.find(sha1Fp);
        if (neighborDedupIter == preTable.end()) {
            return LookupResult::New;
        } else {
            uint32_t oldClass = neighborDedupIter->second.classId;
            *oc = oldClass;
            return LookupResult::NeighborDedup;
        }

    }

    int gcLookup(const SHA1FP &sha1Fp, uint64_t gcVersion) {
        MutexLockGuard mutexLockGuard(tableLock);
        auto targetTableIter = metaTable.find(gcVersion + 1);
        if (targetTableIter == metaTable.end()) {
            assert(1);
        }
        std::unordered_map <SHA1FP, MetaEntry, TupleHasher, TupleEqualer> &targetTable = targetTableIter->second;
        auto r = targetTable.find(sha1Fp);
        if (r == targetTable.end()) {
            return -1;
        } else {
            return 0;
        }
    }

    int newChunkAddRecord(const SHA1FP &sha1Fp, uint64_t currentVersion, const MetaEntry &metaEntry) {
        MutexLockGuard mutexLockGuard(tableLock);
        std::unordered_map <SHA1FP, MetaEntry, TupleHasher, TupleEqualer> &currentTable = metaTable[currentVersion];
        auto pp = currentTable.find(sha1Fp);
        if (pp == currentTable.end()) {
            currentTable[sha1Fp] = metaEntry;
            return 0;
        } else {
            assert(0);
            return -1;
        }
    }

    int neighborAddRecord(const SHA1FP &sha1Fp, uint64_t currentVersion, const MetaEntry &metaEntry) {
        MutexLockGuard mutexLockGuard(tableLock);
        std::unordered_map <SHA1FP, MetaEntry, TupleHasher, TupleEqualer> &currentTable = metaTable[currentVersion];
        auto pp = currentTable.find(sha1Fp);
        if (pp == currentTable.end()) {
            currentTable[sha1Fp] = metaEntry;
            return 0;
        } else {
            assert(0);
            return -1;
        }
    }

    int metatableReleaseLatch(uint64_t targetVersion) {
        MutexLockGuard mutexLockGuard(tableLock);
        auto iter = latchTable.find(targetVersion);
        if (iter == latchTable.end()) {
            latchTable[targetVersion] = 0;
            return 0;
        }
        auto tableIter = metaTable.find(targetVersion);
        if (tableIter == metaTable.end()) {
            assert(0);
        }
        metaTable.erase(tableIter);
        latchTable.erase(targetVersion);

    }

private:
    std::unordered_map <uint64_t, std::unordered_map<SHA1FP, MetaEntry, TupleHasher, TupleEqualer>> metaTable;
    std::unordered_map<uint64_t, int> latchTable;

    MutexLock tableLock;
};

static MetadataManager *GlobalMetadataManagerPtr;

#endif //MFDEDUP_MATADATAMANAGER_H