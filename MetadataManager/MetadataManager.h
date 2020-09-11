//
// Created by Borelset on 2019/7/29.
//

//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_MATADATAMANAGER_H
#define MFDEDUP_MATADATAMANAGER_H

#include <map>
#include "../Utility/StorageTask.h"
#include <unordered_set>
#include <unordered_map>

uint64_t shadMask = 0x7;

int ReplaceThreshold = 10;

extern uint64_t TotalVersion;
extern std::string KVPath;

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
    Unique,
    InternalDedup,
    AdjacentDedup,
    SkipDedup,
};

struct FPIndex{
    uint64_t duplicateSize = 0;
    uint64_t totalSize = 0;
    std::unordered_set<SHA1FP, TupleHasher, TupleEqualer> fpTable;

    void rolling(FPIndex& alter){
        fpTable.clear();
        fpTable.swap(alter.fpTable);
        duplicateSize = alter.duplicateSize;
        totalSize = alter.totalSize;
        alter.duplicateSize = 0;
        alter.totalSize = 0;
    }
};

class MetadataManager {
public:
    MetadataManager() {

    }

    LookupResult dedupLookup(const SHA1FP &sha1Fp, uint64_t chunkSize) {
        MutexLockGuard mutexLockGuard(tableLock);
        auto innerDedupIter = laterTable.fpTable.find(sha1Fp);
        if (innerDedupIter != laterTable.fpTable.end()) {
            return LookupResult::InternalDedup;
        }

        laterTable.totalSize += chunkSize;
        auto neighborDedupIter = earlierTable.fpTable.find(sha1Fp);
        if (neighborDedupIter == earlierTable.fpTable.end()) {

            return LookupResult::Unique;
        } else {
            laterTable.duplicateSize += chunkSize;
            return LookupResult::AdjacentDedup;
        }

    }

    uint64_t arrangementGetTruncateSize(){
        return earlierTable.totalSize - laterTable.duplicateSize;
    }

    int arrangementLookup(const SHA1FP &sha1Fp) {
        MutexLockGuard mutexLockGuard(tableLock);

        auto r = laterTable.fpTable.find(sha1Fp);
        if (r == laterTable.fpTable.end()) {
            return 0;
        } else {
            return 1;
        }
    }

    int newChunkAddRecord(const SHA1FP &sha1Fp) {
        MutexLockGuard mutexLockGuard(tableLock);

        auto pp = laterTable.fpTable.find(sha1Fp);
        assert(pp == laterTable.fpTable.end());

        laterTable.fpTable.insert(sha1Fp);

        return 0;
    }

    int neighborAddRecord(const SHA1FP &sha1Fp) {
        MutexLockGuard mutexLockGuard(tableLock);

        auto pp = laterTable.fpTable.find(sha1Fp);
        assert(pp == laterTable.fpTable.end());

        laterTable.fpTable.insert(sha1Fp);
        return 0;
    }

    int tableRolling() {
        MutexLockGuard mutexLockGuard(tableLock);

        earlierTable.rolling(laterTable);


        return 0;
    }

    int save(){
        printf("------------------------Saving index----------------------\n");
        printf("Saving index..\n");
        uint64_t size;
        FileOperator fileOperator((char*)KVPath.data(), FileOpenType::Write);
        fileOperator.write((uint8_t*)&earlierTable, sizeof(uint64_t)*2);
        size = earlierTable.fpTable.size();
        fileOperator.write((uint8_t*)&size, sizeof(uint64_t));
        for(auto item : earlierTable.fpTable){
            fileOperator.write((uint8_t*)&item, sizeof(SHA1FP));
        }
        printf("earlier table saves %lu items\n", size);
        printf("earlier total size:%lu, duplicate size:%lu\n", earlierTable.totalSize, earlierTable.duplicateSize);
        fileOperator.write((uint8_t*)&laterTable, sizeof(uint64_t)*2);
        size = laterTable.fpTable.size();
        fileOperator.write((uint8_t*)&size, sizeof(uint64_t));
        for(auto item : laterTable.fpTable){
            fileOperator.write((uint8_t*)&item, sizeof(SHA1FP));
        }
        printf("later table saves %lu items\n", size);
        printf("later total size:%lu, duplicate size:%lu\n", laterTable.totalSize, laterTable.duplicateSize);
        fileOperator.fdatasync();
    }

    int load(){
        printf("-----------------------Loading index-----------------------\n");
        printf("Loading index..\n");
        uint64_t sizeE = 0;
        uint64_t sizeL = 0;
        SHA1FP tempFP;
        uint64_t tempCID;
        FileOperator fileOperator((char*)KVPath.data(), FileOpenType::Read);
        assert(earlierTable.fpTable.size() == 0);
        assert(laterTable.fpTable.size() == 0);

        fileOperator.read((uint8_t*)&earlierTable, sizeof(uint64_t)*2);
        fileOperator.read((uint8_t*)&sizeE, sizeof(uint64_t));
        for(uint64_t i = 0; i<sizeE; i++){
            fileOperator.read((uint8_t*)&tempFP, sizeof(SHA1FP));
            earlierTable.fpTable.insert(tempFP);
        }
        printf("earlier table load %lu items\n", sizeE);

        fileOperator.read((uint8_t*)&earlierTable, sizeof(uint64_t)*2);
        fileOperator.read((uint8_t*)&sizeL, sizeof(uint64_t));
        for(uint64_t i = 0; i<sizeL; i++){
            fileOperator.read((uint8_t*)&tempFP, sizeof(SHA1FP));
            laterTable.fpTable.insert(tempFP);
        }
        printf("later table load %lu items\n", sizeL);
    }

private:
    FPIndex earlierTable;
    FPIndex laterTable;

    MutexLock tableLock;
};

static MetadataManager *GlobalMetadataManagerPtr;

#endif //MFDEDUP_MATADATAMANAGER_H