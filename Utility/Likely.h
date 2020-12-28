#ifndef MFDEDUP_LIKELY_H
#define MFDEDUP_LIKELY_H

bool likely(bool input) {
    return __builtin_expect(input, 1);
}

bool unlikely(bool input) {
    return __builtin_expect(input, 0);
}

#endif //MFDEDUP_LIKELY_H
