//  Copyright (c) Xiangyu Zou, 2020. All rights reserved.
//  This source code is licensed under the GPLv2

#ifndef MFDEDUP_RESTOREWRITEPIPELINE_H
#define MFDEDUP_RESTOREWRITEPIPELINE_H

class FileFlusher{
public:
    FileFlusher(FileOperator* f): runningFlag(true), taskAmount(0), mutexLock(), condition(mutexLock), fileOperator(f){
        worker = new std::thread(std::bind(&FileFlusher::fileFlusherCallback, this));
    }

    int addTask(uint64_t task) {
        MutexLockGuard mutexLockGuard(mutexLock);
        taskList.push_back(task);
        taskAmount++;
        condition.notify();
    }

    ~FileFlusher(){
        addTask(-1);
        worker->join();
    }


private:

    void fileFlusherCallback(){
        uint64_t task;
        while (likely(runningFlag)) {
            {
                MutexLockGuard mutexLockGuard(mutexLock);
                while (!taskAmount) {
                    condition.wait();
                    if (unlikely(!runningFlag)) break;
                }
                if (unlikely(!runningFlag)) continue;
                taskAmount--;
                task = taskList.front();
                taskList.pop_front();
            }

            if(task == -1){
                break;
            }

            fileOperator->fdatasync();
        }
    }

    std::thread* worker;
    bool runningFlag;
    uint64_t taskAmount;
    std::list<uint64_t> taskList;
    MutexLock mutexLock;
    Condition condition;
    FileOperator* fileOperator;
};

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
        printf("restore write duration :%lu\n", duration);
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
        FileFlusher fileFlusher(fileOperator);

        struct timeval t0, t1;

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
            gettimeofday(&t0, NULL);

            if (unlikely(restoreWriteTask->endFlag)) {
                delete restoreWriteTask;
                fileOperator->fdatasync();
                countdownLatch->countDown();
                gettimeofday(&t1, NULL);
                duration += (t1.tv_sec-t0.tv_sec)*1000000 + t1.tv_usec - t0.tv_usec;
                break;
            }

            pwrite(fd, restoreWriteTask->buffer, restoreWriteTask->length, restoreWriteTask->pos);

            syncCounter++;
            if(syncCounter > 1024){
                fileFlusher.addTask(1);
                syncCounter = 0;
            }

            delete restoreWriteTask;
            gettimeofday(&t1, NULL);
            duration += (t1.tv_sec-t0.tv_sec)*1000000 + t1.tv_usec - t0.tv_usec;
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

    uint64_t duration = 0;

    uint64_t syncCounter = 0;
};

static RestoreWritePipeline *GlobalRestoreWritePipelinePtr;

#endif //MFDEDUP_RESTOREWRITEPIPELINE_H
