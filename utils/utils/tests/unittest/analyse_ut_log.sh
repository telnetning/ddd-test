#!/bin/bash
# Copyright (C) 2026 Huawei Technologies Co.,Ltd.
#
# dstore is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# dstore is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. if not, see <https://www.gnu.org/licenses/>.

# Regression test case construction for the PageStore.
# This script is used to parse logs for the access control project.
set -e

function collect_log() {
    cp -r ${CMAKE_BUILD_PATH}/error_log/*.log  ${COLLECT_LOG_PATH}
    cp -r ${CMAKE_BUILD_PATH}/cmc/tests/unittest/cmc_ut_logs/*.log   ${COLLECT_LOG_PATH}
    cp -r ${CMAKE_BUILD_PATH}/cmc/tests/unittest/cmc_ut_logs/*/*.log   ${COLLECT_LOG_PATH}
    cp -r ${CMAKE_BUILD_PATH}/cmc/tests/unittest/cmc_ut_logs/*/*/*.log  ${COLLECT_LOG_PATH}
    cp -r ${CMAKE_BUILD_PATH}/cmc/tests/unittest/cmc_ut_logs/*/*/*/*.log  ${COLLECT_LOG_PATH}
    cp -r ${CMAKE_BUILD_PATH}/*.log  ${COLLECT_LOG_PATH}
    cp -r ${CMAKE_BUILD_PATH}/bin/utils_debug/*.log  ${COLLECT_LOG_PATH}
    cp -r ${CMAKE_BUILD_PATH}/utils_debug/*.log  ${COLLECT_LOG_PATH}
}

function main() {
    declare CMAKE_BUILD_PATH=$1
    declare COLLECT_LOG_PATH=${CMAKE_BUILD_PATH}/log/

    mkdir -p ${COLLECT_LOG_PATH}
    set +e
    collect_log
    set -e
}

main "$@"
