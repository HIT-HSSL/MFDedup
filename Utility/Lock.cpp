//
// Created by BorelsetR on 2019/7/17.
//
#include <assert.h>
#include "Lock.h"

void MutexLock::lock() {
    pthread_mutex_lock(&mMutex);
}

void MutexLock::unlock() {
    pthread_mutex_unlock(&mMutex);
}

pthread_mutex_t *MutexLock::getPthreadMutex() {
    return &mMutex;
}

MutexLock::MutexLock() {
    //pthread_mutexattr_settype(&mMutexType, PTHREAD_MUTEX_NORMAL);
    //int r = pthread_mutex_init(&mMutex, &mMutexType);
    int r = pthread_mutex_init(&mMutex, NULL);
    assert(r == 0);
    unlock();
}

MutexLock::~MutexLock() {
    pthread_mutex_destroy(&mMutex);
}

MutexLockGuard::MutexLockGuard(MutexLock &mutex) : mMutexLock(mutex) {
    mMutexLock.lock();
}

MutexLockGuard::~MutexLockGuard() {
    mMutexLock.unlock();
}

Condition::Condition(MutexLock &mutex) : mMutexLock(mutex) {
    pthread_cond_init(&mCond, nullptr);
}

Condition::~Condition() {
    pthread_cond_destroy(&mCond);
}

void Condition::wait() {
    pthread_cond_wait(&mCond, mMutexLock.getPthreadMutex());
}

void Condition::notify() {
    pthread_cond_signal(&mCond);
}

void Condition::notifyAll() {
    pthread_cond_broadcast(&mCond);
}
