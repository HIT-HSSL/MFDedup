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
        FileOperator fileOperator((char*)ManifestPath.data(), FileOpenType::Read);
        if(fileOperator.getStatus() == -1){
            printf("No manifest, build a new one.\n");
            manifest->TotalVersion = 0;
        }else{
            fileOperator.read((uint8_t*)manifest, sizeof(Manifest));
        };
    }
private:
};

#endif //MFDEDUP_MANIFEST_H
