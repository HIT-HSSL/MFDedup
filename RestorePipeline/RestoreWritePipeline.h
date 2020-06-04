//
// Created by Borelset on 2020/5/27.
//
//  Copyright (C) 2020-present, Xiangyu Zou. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_RESTOREWRITEPIPELINE_H
#define MFDEDUP_RESTOREWRITEPIPELINE_H

class RestoreWritePipeline {
public:
    RestoreWritePipeline(std::string restorePath, CountdownLatch *cd) : taskAmount(0), runningFlag(true), mutexLock(),
                                               condition(mutexLock), countdownLatch(cd) {
        fileOperator = new FileOperator((char*)restorePath.data(), FileOpenType::Write);
        worker = new std::thread(std::bind(&RestoreWritePipeline::restoreWriteCallback, this));
    }

    int addTask(RestoreWriteTask *restoreWriteTask) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(restoreWriteTask);
        taskAmount++;
        condition.notify();
    }

    ~RestoreWritePipeline() {
        runningFlag = false;
        condition.notifyAll();
        worker->join();
    }

    int setSize(uint64_t size){
        totalSize = size;
        if (fileOperator){
            fileOperator->trunc(size);
        }
    }

    uint64_t getTotalSize(){
        return totalSize;
    }

private:
    void restoreWriteCallback() {
        RestoreWriteTask *restoreWriteTask;
        int fd = fileOperator->getFd();

        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                restoreWriteTask = taskList.front();
                taskList.pop_front();
            }

            if (unlikely(restoreWriteTask->endFlag)) {
                delete restoreWriteTask;
                fileOperator->fdatasync();
                countdownLatch->countDown();
                break;
            }

            pwrite(fd, restoreWriteTask->buffer, restoreWriteTask->length, restoreWriteTask->pos);

            delete restoreWriteTask;
        }
    }


    CountdownLatch *countdownLatch;
    bool runningFlag;
    std::thread *worker;
    uint64_t taskAmount;
    std::list<RestoreWriteTask *> taskList;
    MutexLock mutexLock;
    Condition condition;
    FileOperator* fileOperator = nullptr;

    uint64_t totalSize = 0;
};

static RestoreWritePipeline *GlobalRestoreWritePipelinePtr;

#endif //MFDEDUP_RESTOREWRITEPIPELINE_H
