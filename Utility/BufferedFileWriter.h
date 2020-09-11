//
// Created by Borelset on 2020/6/23.
//
//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_BUFFEREDFILEWRITER_H
#define MFDEDUP_BUFFEREDFILEWRITER_H

class BufferedFileWriter {
public:
    BufferedFileWriter(FileOperator *fd, uint64_t size, uint64_t st) : fileOperator(fd), bufferSize(size), syncThreshold(st) {
        writeBuffer = (uint8_t *) malloc(bufferSize);
        writeBufferAvailable = bufferSize;
    }

    int write(uint8_t *data, int dataLen) {
        if (dataLen > writeBufferAvailable) {
            flush();
        }
        uint8_t *writePoint = writeBuffer + bufferSize - writeBufferAvailable;
        memcpy(writePoint, data, dataLen);
        writeBufferAvailable -= dataLen;
        return 0;
    }

    ~BufferedFileWriter() {
        counter += syncThreshold;
        flush();
        free(writeBuffer);
    }

private:

    int flush() {
        fileOperator->write(writeBuffer, bufferSize - writeBufferAvailable);
        writeBufferAvailable = bufferSize;
        counter++;
        if(counter >= syncThreshold){
            fileOperator->fdatasync();
            counter = 0;
        }
    }

    uint64_t bufferSize;
    uint8_t *writeBuffer;
    int writeBufferAvailable;
    FileOperator *fileOperator;

    uint64_t counter = 0;
    uint64_t syncThreshold = 0;
};

#endif //MFDEDUP_BUFFEREDFILEWRITER_H
