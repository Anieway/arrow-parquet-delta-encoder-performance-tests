cmake_minimum_required(VERSION 3.5)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

project( ParquetTimingTest )

if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()

set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

find_package(Parquet REQUIRED)

link_libraries( Threads::Threads )
link_libraries( Parquet::parquet_static )

add_library(EncoderRoundtrip STATIC src/EncoderRoundtripTest.cpp)

#This is just a playground atm TODO: move relevant parts of working tests to a git-repository
add_executable( EncoderThroughput src/EncoderThroughput.cpp )
target_link_libraries(EncoderThroughput EncoderRoundtrip)

add_executable( EncoderRandThroughput src/EncoderRandThroughput.cpp )
target_link_libraries(EncoderRandThroughput EncoderRoundtrip)

add_executable( EncoderTestFromFile src/EncoderTestFromParquetData.cpp )
target_link_libraries(EncoderTestFromFile EncoderRoundtrip)

add_executable( MemcpyPositiveTest src/MemcpyPositivtest.cpp)