//
// Created by Borelset on 2020/6/23.
//
//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_BUFFEREDFILEWRITER_H
#define MFDEDUP_BUFFEREDFILEWRITER_H

class BufferedFileWriter {
public:
    BufferedFileWriter(FileOperator *fd, uint64_t size) : fileOperator(fd), bufferSize(size) {
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
        flush();
        free(writeBuffer);
    }

private:

    int flush() {
        fileOperator->write(writeBuffer, bufferSize - writeBufferAvailable);
        writeBufferAvailable = bufferSize;
    }

    uint64_t bufferSize;
    uint8_t *writeBuffer;
    int writeBufferAvailable;
    FileOperator *fileOperator;
};

#endif //MFDEDUP_BUFFEREDFILEWRITER_H
