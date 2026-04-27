#!/bin/bash
# Build CloudNativeDatabaseUtils repository.
# Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
set -e

origin_work_directory=$(pwd)

# default cmake argument
cmake_build_opt=""

if [ -z "$LOCAL_LIB_PATH" ]; then
    echo " can not find LOCAL_LIB_PATH!" >&2
    exit 1
fi

# default options
declare ROOT_DIR=$(cd $(dirname $0) && pwd)
declare SCRIPT_DIR=${ROOT_DIR}/scripts
declare compile_mode='release'
declare sys_tools='off'
declare separate_symbol='on'
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
    -vb|--verbose           if set compile with cmake log.
    -tm|--test_mode         set test mode ut/fuzz/perf/tpcc/lcov.
    -o|--output             set compile result path
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
        -o|--output)
            if [ "$2"X = X ]; then
                echo "[ERROR]no output path provided"
                exit 1
            fi
            install_dir=$2
            shift 2
            ;;
        --component)
            component="$2"
            echo "[INFO]build componet $2"
            shift 2
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

source ${SCRIPT_DIR}/common.sh
source ${SCRIPT_DIR}/compile.sh

build_utils
cd ${origin_work_directory}