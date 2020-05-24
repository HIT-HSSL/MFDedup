//
// Created by BorelsetR on 2019/7/16.
//

#ifndef REDUNDANCY_DETECTION_GEAR_H
#define REDUNDANCY_DETECTION_GEAR_H

#include <cstring>
#include "RollHash.h"
#include "md5.h"

const uint32_t SymbolTypes = 256;
const uint32_t MD5Length = 16;
const int SeedLength = 64;

class Gear : public RollHash {
public:
    Gear() : hashValue(0) {
        char seed[SeedLength];
        for (int i = 0; i < SymbolTypes; i++) {
            for (int j = 0; j < SeedLength; j++) {
                seed[j] = i;
            }

            gearMatrix[i] = 0;
            char md5_result[MD5Length];
            md5_state_t md5_state;
            md5_init(&md5_state);
            md5_append(&md5_state, (md5_byte_t *) seed, SeedLength);
            md5_finish(&md5_state, (md5_byte_t *) md5_result);

            memcpy(&gearMatrix[i], md5_result, sizeof(uint64_t));
        }
    }

    virtual inline uint64_t rolling(uint8_t *inputPtr) override {
        hashValue = hashValue << (uint8_t) 1;
        uint8_t test = *inputPtr;
        hashValue += gearMatrix[*inputPtr];
        return hashValue;
    }

    virtual uint64_t reset() override {
        hashValue = 0;
        return 0;
    }

    virtual uint64_t getDeltaMask() override {
        return 28;
        //return 0x0000d90303530000;
    }

    virtual uint64_t getChunkMask() override {
        return 0x0000d90303530000;
    }

    virtual bool tryBreak(uint64_t fp) override {
        return !(fp & getChunkMask());
    }

    virtual uint64_t *getMatrix() {
        return gearMatrix;
    }

private:
    uint64_t gearMatrix[SymbolTypes];
    uint64_t hashValue;
};

#endif //REDUNDANCY_DETECTION_GEAR_H
