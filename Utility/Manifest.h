//
// Created by Borelset on 2020/6/19.
//

#ifndef MFDEDUP_MANIFEST_H
#define MFDEDUP_MANIFEST_H

#include <string>
#include "FileOperator.h"

struct Manifest{
    uint64_t TotalVersion;
};

extern std::string ManifestPath;

class ManifestWriter{
public:
    ManifestWriter(const Manifest& manifest){
        FileOperator fileOperator((char*)ManifestPath.data(), FileOpenType::Write);
        fileOperator.write((uint8_t*)&manifest, sizeof(Manifest));
    }
private:
};

class ManifestReader{
public:
    ManifestReader(struct Manifest* manifest){
        printf("-----------------------Manifest-----------------------\n");
        printf("Loading Manifest..\n");
        FileOperator fileOperator((char*)ManifestPath.data(), FileOpenType::Read);
        if(fileOperator.getStatus() == -1){
            printf("0 version in storage\n");
            manifest->TotalVersion = 0;
        }else{
            fileOperator.read((uint8_t*)manifest, sizeof(Manifest));
            printf("%lu versions in storage\n", manifest->TotalVersion);
        };
    }
private:
};

#endif //MFDEDUP_MANIFEST_H
