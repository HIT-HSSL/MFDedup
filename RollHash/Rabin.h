//
// Created by BorelsetR on 2019/7/16.
//

#ifndef REDUNDANCY_DETECTION_RABIN_H
#define REDUNDANCY_DETECTION_RABIN_H

#include <cstring>
#include "RollHash.h"
#include "rabin_chunking.h"

const int RabinBufferSize = 128;

class Rabin : public RollHash {
public:
    Rabin() {
        chunkAlg_init();
        memset(rabin_buf, 0, RabinBufferSize);
    }

    virtual inline uint64_t rolling(uint8_t *inputPtr) override {
        unsigned char om;
        uint64_t x;
        if (++bufPos >= size)
            bufPos = 0;
        om = rabinBuf[bufPos];
        rabinBuf[bufPos] = *inputPtr;
        hashValue ^= U[om];
        x = hashValue >> shift;
        hashValue <<= 8;
        hashValue |= *inputPtr;
        hashValue ^= T[x];
        return hashValue;
    }

    virtual uint64_t reset() override {
        rabin_local_init();
        hashValue = 0;
        bufPos = 0;
        memset(rabinBuf, 0, RabinBufferSize);
        return 0;
    }

    virtual uint64_t getDeltaMask() override {
        return 28;
    }

    virtual uint64_t getChunkMask() override {
        return rabin_masks();
    }

    virtual bool tryBreak(uint64_t fp) override {
        return (fp & getChunkMask()) == rabin_break_value();
    }


private:
    uint64_t hashValue = 0;
    int bufPos = 0;
    unsigned char rabinBuf[RabinBufferSize];

};

#endif //REDUNDANCY_DETECTION_RABIN_H
