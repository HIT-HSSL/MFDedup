cmake_minimum_required(VERSION 3.10)
project(MFDedup)

set(CMAKE_CXX_STANDARD 14)

aux_source_directory(Utility Utility)
aux_source_directory(RollHash RollHash)
aux_source_directory(MetadataManager MetadataManager)
aux_source_directory(DedupPipeline DedupPipeline)
aux_source_directory(RestorePipeline RestorePipeline)
add_subdirectory(gflags-2.2.2)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")

link_libraries(gflags::gflags isal_crypto pthread crypto jemalloc)

add_executable(MFDedup main.cpp ${Utility} ${RollHash} ${MetadataManager} ${Pipeline} ${RestorePipeline})
add_executable(readTest Test/readTest.cpp)