#ifndef REDUNDANCY_DETECTION_NONCOPYABLE_H
#define REDUNDANCY_DETECTION_NONCOPYABLE_H

class noncopyable {
protected:
    noncopyable() = default;

    ~noncopyable() = default;

private:
    noncopyable(const noncopyable &) = delete;

    const noncopyable &operator=(const noncopyable &) = delete;
};

#endif //REDUNDANCY_DETECTION_NONCOPYABLE_H
