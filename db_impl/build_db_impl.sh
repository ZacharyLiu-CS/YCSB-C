#!/bin/bash
PROJ_PATH=$(dirname "$0")
PROJ_PATH=$(realpath $PROJ_PATH)
echo "Running into: "$PROJ_PATH
CORE_NUM=$((`nproc`-1))
echo "cpu cores: $CORE_NUM"
cd ${PROJ_PATH}/codebase && git checkout remote_meory && git pull 
cd ${PROJ_PATH}/codebase && mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build . -j ${CORE_NUM}



