#ifndef REDUNDANCY_DETECTION_LOCK_H
#define REDUNDANCY_DETECTION_LOCK_H

#include <iostream>
#include "pthread.h"
#include "Noncopyable.h"

class MutexLockGuard;

class Condition;

class MutexLock : noncopyable {
private:
    pthread_mutex_t mMutex;
    pthread_mutexattr_t mMutexType;
    friend MutexLockGuard;
    friend Condition;


    pthread_mutex_t *getPthreadMutex();

public:
    void lock();

    void unlock();

    MutexLock();

    ~MutexLock();
};

class MutexLockGuard : noncopyable {
private:
    MutexLock &mMutexLock;
public:
    explicit MutexLockGuard(MutexLock &mutex);

    ~MutexLockGuard();
};

class Condition : noncopyable {
private:
    MutexLock &mMutexLock;
    pthread_cond_t mCond;
public:
    explicit Condition(MutexLock &mutex);

    ~Condition();

    void wait();

    void notify();

    void notifyAll();
};

class CountdownLatch : noncopyable {
public:
    CountdownLatch(int n) : mutexLock(), condition(mutexLock), count(n) {

    }

    void wait() {
        MutexLockGuard mutexLockGuard(mutexLock);
        while (count > 0) {
            condition.wait();
        }
    }

    void addCount() {
        MutexLockGuard mutexLockGuard(mutexLock);
        ++count;
    }

    void setCount(int n) {
        MutexLockGuard mutexLockGuard(mutexLock);
        count = n;
    }

    void countDown() {
        MutexLockGuard mutexLockGuard(mutexLock);
        --count;
        if (count == 0) {
            condition.notifyAll();
        }
    }

private:
    MutexLock mutexLock;
    Condition condition;
    int count;
};

#endif //REDUNDANCY_DETECTION_LOCK_H
