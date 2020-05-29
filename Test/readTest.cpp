//
// Created by Borelset on 2020/5/29.
//



#include <cstdio>
#include <cstdint>
#include <cstdlib>

int main(){
    FILE* file = fopen("/home/zxy/MFDedupHome/storageFiles/11", "rb");

    uint64_t size = 5000000000;

    uint8_t* buffer = (uint8_t*)malloc(size);

    fread(buffer, size, 1, file);

    printf("%c\n", buffer[size-1]);
}
