//
// Created by BorelsetR on 2019/7/16.
//

#ifndef REDUNDANCY_DETECTION_ROLLHASH_H
#define REDUNDANCY_DETECTION_ROLLHASH_H

#include <functional>

enum class HashType {
    Rabin,
    Gear,
};

class RollHash {
public:
    virtual inline uint64_t rolling(uint8_t *inputPtr) {
        printf("RollHash error encountered\n");
        return 0;
    }

    virtual uint64_t reset() {
        printf("RollHash error encountered\n");
        return 0;
    }

    virtual uint64_t getDeltaMask() {
        printf("RollHash error encountered\n");
        return 0;
    }

    virtual uint64_t getChunkMask() {
        printf("RollHash error encountered\n");
        return 0;
    }

    virtual bool tryBreak(uint64_t fp) {
        printf("RollHash error encountered\n");
        return 0;
    }

    virtual uint64_t *getMatrix() {
        printf("RollHash error encountered\n");
        return 0;
    }
};

#endif //REDUNDANCY_DETECTION_ROLLHASH_H
