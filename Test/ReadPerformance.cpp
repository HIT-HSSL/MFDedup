#include <sys/time.h>
#include <assert.h>
#include "gflags/gflags.h"

DEFINE_string(Path,
              "", "read path");

int main(int argc, char **argv){
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    FILE* file = fopen(FLAGS_Path.data(), "rb");
    int r = fseeko64(file, 0, SEEK_END);
    assert(r == 0);
    uint64_t size = ftell(file);
    r = fseeko64(file, 0, SEEK_SET);
    assert(r == 0);

    uint8_t* buffer = (uint8_t*)malloc(size);

    struct timeval t0, t1;
    gettimeofday(&t0, NULL);
    uint64_t itemCount = fread(buffer, size, 1, file);
    assert(itemCount == 1);
    gettimeofday(&t1, NULL);
    uint64_t duration = (t1.tv_sec-t0.tv_sec) * 1000000 + (t1.tv_usec - t0.tv_usec);
    printf("fread() speed : %f MB/s\n", (float)size / duration);

    free(buffer);
    fclose(file);
}