#!/bin/bash
PROJ_PATH=$(dirname "$0")
PROJ_PATH=$(realpath $PROJ_PATH)
echo "Running into: "$PROJ_PATH
cd ${PROJ_PATH}/googletest && mkdir -p build && cd build && cmake .. && make -j
cd ${PROJ_PATH}/yaml-cpp && mkdir -p build && cd build && cmake .. && make -j


