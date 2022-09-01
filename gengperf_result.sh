#!/bin/bash
#  gengperf_result.sh
#
#  Created by zhenliu on 01/09/2022.
#  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
#
PROJ_PATH=$(cd `dirname $0`; pwd)
google-pprof ${PROJ_PATH}/build/ycsbc load_ycsbc_phase.prof --pdf > ${PROJ_PATH}/build/load_ycsbc.pdf
google-pprof ${PROJ_PATH}/build/ycsbc run_ycsbc_phase.prof --pdf > ${PROJ_PATH}/build/run_ycsbc.pdf
