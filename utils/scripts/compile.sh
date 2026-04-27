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

# Build CloudNativeDatabaseUtils repository.

set -e

function clean_directory()
{
    [ -n "${build_path}" ] && rm -rf ${build_path}
    [ -n "${install_dir}" ] && rm -rf ${install_dir}
    if [[ -z "$test_mode" ]] && [[ ! "${extra_cmake_opt}" =~ "-DENABLE_OBJDIFF=ON" ]]; then
        [ -n "${ARCHIVE_DIR}" ] && rm -rf ${ARCHIVE_DIR}
    fi
}

function make_and_clean_directory()
{
    clean_directory
    mkdir -p ${build_path}
    mkdir -p ${install_dir}
    if [[ -z "$test_mode" ]]; then
        mkdir -p ${ARCHIVE_DIR}
    fi
}

function get_os_name()
{
    temp_os_name=$(cat /etc/os-release | grep NAME | grep -v PRETTY | awk -F "\"" '{print $2}')
    if [ "${temp_os_name}" = "" ]; then
        log "[WARNING] Get os name failed"
    else
        package_os_name=${temp_os_name}
    fi
}

function get_project_version()
{
    temp_version_file="$1/VERSION"
    temp_project_version=$(cat "${temp_version_file}" | grep GAUSSDB_UTILS_VERSION | awk -F "=" '{print $2}')
    if [ "${temp_project_version}" = "" ]; then
        log "[WARNING]Get project version failed"
    else
        package_version=${temp_project_version}
    fi
}

function build_component()
{
    case "$1" in
        all)
            make -j "$job_num"
            if [ $? -ne 0 ]; then
                die "compile utils filed!"
            fi
            make install -j "$job_num"
            ;;
        utils)
            make -j "$job_num" -C utils && make -j "$job_num" -C vfs
            cmake -P utils/cmake_install.cmake && cmake -P vfs/cmake_install.cmake
            ;;
        communication)
            make -j "$job_num" -C communication
            cmake -P communication/cmake_install.cmake
            ;;
        ?)
            log "[ERROR]unkown build component"
            ;;
    esac
}

function build_component_objdiff()
{
    case "$1" in
        all)
            make -i -j "$job_num"
            ;;
        utils)
            make -i -j "$job_num" -C utils && make -i -j "$job_num" -C vfs
            ;;
        communication)
            make -i -j "$job_num" -C communication
            ;;
        ?)
            log "[ERROR]unkown build component"
            ;;
    esac
}

function build_utils()
{
    if [ "${clean_only}" = true ]; then
        clean_directory
        log "====== Utils clean success ======"
        exit 0
    fi

    make_and_clean_directory
    log "====== Utils build start ======"

    cd ${build_path}
    log "[INFO] Build directory:${build_path}"
    export DEBUG_TYPE=${compile_mode}
    # export THIRD_BIN_PATH="${binarylib_dir}"

    cmake_build_opt+=" -DLOCAL_LIB_PATH=$LOCAL_LIB_PATH -DSTATIC_SSL=ON"
    cmake_build_opt+=" -DCMAKE_INSTALL_PREFIX=${install_dir}"

    if [ "${test_mode}"X = "ut"X ]; then
        cmake_build_opt+=" -DENABLE_UT=ON"
    elif [ "${test_mode}"X = "fuzz"X ]; then
        cmake_build_opt+=" -DENABLE_UT=ON -DENABLE_FUZZ=ON"
    elif [ "${test_mode}"X = "perf"X ]; then
        cmake_build_opt+=" -DENABLE_UT=ON -DENABLE_PERF=ON"
    elif [ "${test_mode}"X = "lcov"X ]; then
        cmake_build_opt+=" -DCMAKE_BUILD_TYPE=debug -DENABLE_UT=ON -DENABLE_LCOV=ON"
    fi

    cmake ${ROOT_DIR} ${cmake_build_opt} ${extra_cmake_opt}
    if [[ "${extra_cmake_opt}" =~ "-DENABLE_OBJDIFF=ON" ]]; then
        build_component_objdiff ${component}
    else
        build_component ${component}
    fi
    if [[ -z "$test_mode" ]] && [[ ! "${extra_cmake_opt}" =~ "-DENABLE_OBJDIFF=ON" ]]; then
        cp -r ${install_dir}/* ${ARCHIVE_DIR}
    fi
    log "====== Utils build success ======"
}