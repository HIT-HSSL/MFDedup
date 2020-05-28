//
// Created by Borelset on 2020/5/27.
//

#ifndef MFDEDUP_RESTOREWRITEPIPELINE_H
#define MFDEDUP_RESTOREWRITEPIPELINE_H

class RestoreWritePipeline {
public:
    RestoreWritePipeline(CountdownLatch *cd) : taskAmount(0), runningFlag(true), mutexLock(),
                                               condition(mutexLock), countdownLatch(cd) {
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

private:
    void restoreWriteCallback() {
        RestoreWriteTask *restoreWriteTask;

        while (runningFlag) {
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
                countdownLatch->countDown();
                break;
            }

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
};

static RestoreWritePipeline *GlobalRestoreWritePipelinePtr;

#endif //MFDEDUP_RESTOREWRITEPIPELINE_H
