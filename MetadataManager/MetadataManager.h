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

extern uint64_t TotalVersion;

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

class MetadataManager {
public:
    MetadataManager() {

    }

    LookupResult dedupLookup(const SHA1FP &sha1Fp, uint64_t currentVersion, uint64_t *oc) {
        MutexLockGuard mutexLockGuard(tableLock);
        auto innerDedupIter = laterTable.find(sha1Fp);
        if (innerDedupIter != laterTable.end()) {
            return LookupResult::InnerDedup;
        }

        auto neighborDedupIter = earlierTable.find(sha1Fp);
        if (neighborDedupIter == earlierTable.end()) {
            return LookupResult::New;
        } else {
            *oc = neighborDedupIter->second;
            return LookupResult::NeighborDedup;
        }

    }

    int gcLookup(const SHA1FP &sha1Fp, uint64_t gcVersion) {
        MutexLockGuard mutexLockGuard(tableLock);


        auto r = laterTable.find(sha1Fp);
        if (r == laterTable.end()) {
            return -1;
        } else {
            return 0;
        }
    }

    int newChunkAddRecord(const SHA1FP &sha1Fp, uint64_t currentVersion, uint64_t classId) {
        MutexLockGuard mutexLockGuard(tableLock);

        auto pp = laterTable.find(sha1Fp);
        assert(pp == laterTable.end());

        laterTable[sha1Fp] = classId;
        return 0;
    }

    int neighborAddRecord(const SHA1FP &sha1Fp, uint64_t currentVersion, uint64_t classId) {
        MutexLockGuard mutexLockGuard(tableLock);

        auto pp = laterTable.find(sha1Fp);
        assert(pp == laterTable.end());

        laterTable[sha1Fp] = classId;
        return 0;
    }

    int tableRolling() {
        MutexLockGuard mutexLockGuard(tableLock);

        earlierTable.clear();
        earlierTable.swap(laterTable);


        return 0;
    }

    int updateMetaTableAfterDeletion(){
        MutexLockGuard mutexLockGuard(tableLock);


        uint64_t startClass = (TotalVersion - 1) * TotalVersion / 2 + 1;
        uint64_t endClass = (TotalVersion + 1) * TotalVersion / 2;

        std::unordered_map<SHA1FP, uint64_t, TupleHasher, TupleEqualer> alterTable;

        for(auto item : earlierTable){
            assert(item.second-startClass <= endClass-startClass);
            if(item.second == startClass){
                alterTable[item.first] = item.second - (TotalVersion-1);
            }else{
                alterTable[item.first] = item.second - (TotalVersion);
            }
        }
        earlierTable.swap(alterTable);
    }

private:
    std::unordered_map<SHA1FP, uint64_t, TupleHasher, TupleEqualer> earlierTable;
    std::unordered_map<SHA1FP, uint64_t, TupleHasher, TupleEqualer> laterTable;

    MutexLock tableLock;
};

static MetadataManager *GlobalMetadataManagerPtr;

#endif //MFDEDUP_MATADATAMANAGER_H