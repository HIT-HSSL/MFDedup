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
        auto iter = metaTable.find(sha1Fp);
        if (iter != metaTable.end()) {
            return LookupResult::InnerDedup;
        } else {
            return LookupResult::New;
        }
    }

    int newChunkAddRecord(const SHA1FP &sha1Fp, uint64_t currentVersion, const MetaEntry &metaEntry) {
        MutexLockGuard mutexLockGuard(tableLock);
        metaTable[sha1Fp] = nullptr;
    }

private:
    std::unordered_map<SHA1FP, MetaEntry *, TupleHasher, TupleEqualer> metaTable;

    MutexLock tableLock;
};

static MetadataManager *GlobalMetadataManagerPtr;

#endif //MFDEDUP_MATADATAMANAGER_H