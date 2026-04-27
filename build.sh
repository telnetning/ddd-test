#!/usr/bin/env bash

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

set -e

origin_work_directory=$(pwd)

# default cmake argument
cmake_build_opt=""

if [ -z "$LOCAL_LIB_PATH" ]; then
    echo " can not find LOCAL_LIB_PATH!" >&2
    exit 1
fi

# default options
declare SCRIPT_DIR=$(cd $(dirname "$0") && pwd)
declare compile_mode='release'
declare sys_tools='off'
declare separate_symbol='on'
declare ROOT_DIR=${SCRIPT_DIR}
declare install_dir=${ROOT_DIR}/output
clean_only=false
component=all

function print_help()
{
    echo "Usage: $0 [OPTION]
    -h|--help               show help information.
    -m|--compile_mode       default 'release'; this values of paramenter is debug\release\memcheck\coverage(release).
    -st|--sys_tools         default 'off'; get buildtools from system when the value is 'on'. on/off
    -co|--cmake_opt         more config options.
    -vb|--verbose          if set compile with cmake log.
    -tm|--test_mode         set test mode ut/fuzz/perf/tpcc/lcov.
    "
}

# parse arguments
while [ $# -gt 0 ]; do
    case "$1" in
        clean|--clean)
            clean_only=true
            echo "[INFO]Just clean build directory"
            shift
            ;;
        -m|--compile_mode)
            if [ "$2"X = X ]; then
                echo "[INFO]Set compile mode debug release or memcheck, coverage"
            fi
            compile_mode=${2}
            shift 2
            ;;
        -st|--sys_tools)
            if [ "$2"X = X ]; then
                echo "default 'off'; get buildtools from system when the value is 'on'."
            fi
            sys_tools=$2
            shift 2
            ;;
        -co|--cmake_opt)
            if [ "$2"X = X ]; then
                echo "[ERROR]no extra configure options provided"
                exit 1
            fi
            extra_cmake_opt+=" $2"
            shift 2
            ;;
        -vb|--verbose)
            VERBOSE="VERBOSE=1"
            shift 
            ;;
        -h|--help)
            print_help
            exit 0
            ;;
        *)
            echo "[ERROR]Unknown build parameters: $1"
            exit 1
            ;;
    esac
done

export LOCAL_LIB_PATH=${LOCAL_LIB_PATH}
source ${SCRIPT_DIR}/scripts/common.sh
source ${SCRIPT_DIR}/scripts/compile.sh

build_dstore
cd ${origin_work_directory}
