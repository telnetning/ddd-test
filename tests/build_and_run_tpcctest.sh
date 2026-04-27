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

current_file_path="$(cd "$(dirname "$0")" && pwd)"
buidcache_dir="${current_file_path}/../tmp_build"
dstore_tpcctest_bin_dir="${buidcache_dir}/bin"
cpus_num=$(grep -w processor /proc/cpuinfo|wc -l)

function usage()
{
    echo "Usage:"
    echo " $(basename "$0") -t|--local_lib <path> -u|utils_path <path>"
    echo "Options:"
    echo " -t, --local_lib <path>         The local_lib path."
    echo " -u, --utils_path <path>        The utils output path."
    echo " -h, --help                     Get help info."
    echo " -a, --asan_mode                Open asan: ON(default), OFF."
    echo " -r, --rebuild                  Always rebuild before run ut, default is true."
}

function check_param()
{
    if [ -z "${local_lib}" ]; then
        echo "Local lib path empty!"
        exit 1
    fi

    if [ -z "${utils_path}" ]; then
        echo "The utils path empty!"
        exit 1
    fi
}

function build_tpcctest()
{
    if [ -d "${buidcache_dir}" ]; then
        rm -rf "${buidcache_dir}"
    fi
    mkdir -p "${buidcache_dir}"
    cd "${buidcache_dir}" || exit
    if [ ${asan_mode} == "ON" ]; then
        cmake .. -DCMAKE_BUILD_TYPE=memcheck       \
                 -DLOCAL_LIB_PATH="${local_lib}"   \
                 -DUTILS_PATH="${utils_path}"      \
                 -DDSTORE_TEST_TOOL=ON
    else
        cmake .. -DLOCAL_LIB_PATH="${local_lib}"   \
                 -DUTILS_PATH="${utils_path}"      \
                 -DDSTORE_TEST_TOOL=ON
    fi
    make -j${cpus_num} install
	
	sed -i 's/"buffer": 300000/"buffer": 655360/g' guc.json
}

function run_tpcctest()
{
    export LD_LIBRARY_PATH=${utils_path}/lib:$LD_LIBRARY_PATH
    export LD_LIBRARY_PATH=${local_lib}/lib:$LD_LIBRARY_PATH
    
    export LD_LIBRARY_PATH=${utils_path}/lib:$LD_LIBRARY_PATH
    export LD_LIBRARY_PATH=${local_lib}/lib:$LD_LIBRARY_PATH
    
    echo $LD_LIBRARY_PATH | tr ':' '\n'
    file "${dstore_tpcctest_bin}"
	if [ ! -d "${dstore_tpcctest_bin_dir}" ]; then
		echo "Error: Directory ${dstore_tpcctest_bin_dir} does not exist"
		exit 1
	fi

	# Clear the data records
	rm -rf tpccdir
	sleep 0.1s
	
    echo "run dstore one-node tpcctest."
    "${dstore_tpcctest_bin}"
}

function main()
{
    dstore_tpcctest_bin="${dstore_tpcctest_bin_dir}/tpcctest"
    log_path=""
    root_db_type="dcc"
    env_type="debug"
    local_lib=""
    gausshome=""
    dstorehome=""
    asan_mode="ASAN"
    rebuild="true"
    getopt_cmd=$(getopt -o t:u:a:r:hl: -l local_lib:,utils_path:,asan_mode:,rebuild:,help -n "$(basename "$0")" -- "$@")
    eval set -- "$getopt_cmd"
    while [ -n "${1}" ]
    do
        case "${1}" in
        -t|--local_lib)
            local_lib="${2}"
            echo "build_run_tpcctest:local_lib:${local_lib}"
            shift 2
        ;;
        -u|--utils_path)
            utils_path="${2}"
            echo "build_run_tpcctest:utils_path:${utils_path}"
            shift 2
        ;;
        -a|--asan_mode)
            asan_mode="${2}"
            echo "build_run_tpcctest:asan_mode:${asan_mode}"
            shift 2
        ;;
        -r|--rebuild)
            rebuild="${2}"
            echo "build_run_tpcctest:rebuild:${rebuild}"
            shift 2
        ;;
        -h|--help)
            usage
            shift
            exit 0
        ;;
        --)
            shift
            break
        ;;
        *)
            echo "Error: ${1}"
            usage
            exit 1
        esac
    done

    check_param
    if [ "${rebuild}" == "true" ]; then
        build_tpcctest
    fi
    run_tpcctest
    result=$?
    return ${result}
}

main "$@"
