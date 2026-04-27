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

# Use to check or fix format of source file by clang-format tool

CLANG_FORMAT=${CLANG_FORMAT_PATH}
readonly CHECK_DIRS=(
    utils/interface
    utils/include
    utils/src
    vfs/include
    vfs/src
    communication/interface
    communication/include
    communication/src
    )
readonly LAST_CHANGED_FILES=$(git diff --name-only HEAD~ HEAD ${CHECK_DIRS[@]} |grep '\.[ch]$')

function err_echo()
{
    echo "[$(date +'%Y-%m-%d %H:%M:%S')]$*" >&2
}

function is_clang_format_exist()
{
    if [ ! -f ${CLANG_FORMAT} ]; then
        err_echo "[${FUNCNAME}:${LINENO}]" "Cannot access ${CLANG_FORMAT}, not such file."
        return 1
    fi
}

function format_fix_one_file()
{
    local fix_file=$1
    if ! ${CLANG_FORMAT} --dry-run -Werror "$fix_file" 2>/dev/null; then
        ${CLANG_FORMAT} -i $fix_file
        echo "Fix format success:" "$fix_file"
    else
        echo "Format is OK:" "$fix_file"
    fi
}

function format_fix_files()
{
    while [ $# != 0 ]
    do
        format_fix_one_file $1
        shift
    done
}

function format_fix_last_commit()
{
    echo "do last commit format checking"
    for changed_file in $LAST_CHANGED_FILES; do
        format_fix_one_file $changed_file
    done
}

function format_fix_directory()
{
    local dir_files=$(find $* -type f -a '(' -name '*.c' -o -name '*.h' ')')
    for fix_file in $dir_files; do
        format_fix_one_file $fix_file
    done
}

has_format_error=0
function format_check_one_file()
{
    local check_file=$1
    if ! ${CLANG_FORMAT} --dry-run -Werror "$check_file" 2>/dev/null; then
        has_format_error=1
        echo "Format checking error: $check_file has unformatted code."
        ${CLANG_FORMAT} --dry-run -Werror "$check_file" || true
    else
        echo "Format checking success:" "$check_file"
    fi
}

function format_check_files()
{
    while [ $# != 0 ]
    do
        format_check_one_file $1
        shift 1
    done
}

function format_check_last_commit()
{
    for changed_file in $LAST_CHANGED_FILES; do
        format_check_one_file $changed_file
    done
}

function format_check_directory()
{
    local dir_files=$(find $* -type f -a '(' -name '*.c' -o -name '*.h' ')')
    for check_file in $dir_files; do
        format_check_one_file $check_file
    done
}

function format_check()
{
    case "$1" in
        all|--all)
            format_check_directory ${CHECK_DIRS[@]}
            ;;
        last|--last-commit)
            format_check_last_commit
            ;;
        dir|--directory)
            format_check_directory $2
            ;;
        files|--files)
            shift 1
            format_check_files $*
            ;;
    esac
}

function format_fix()
{
    case "$1" in
        all|--all)
            format_fix_directory ${CHECK_DIRS[@]}
            ;;
        last|--last-commit)
            format_fix_last_commit
            ;;
        dir|--directory)
            format_fix_directory $2
            ;;
        files|--files)
            shift 1
            format_fix_files $*
            ;;
    esac
}

function show_help()
{
    echo "NAME"
    echo "    it is a script use for format source code by clang-format"
    echo "SYNOPSIS"
    echo "    sh clang-format.sh [OPTION]"
    echo "DESCRIPTION"
    echo "    bin_path,--bin_path"
    echo "        specify the path of clang-format binary file"
    echo "        note that, need to specify the path as the first parameter"
    echo "    check,--check"
    echo "        check the format of source files"
    echo "    fix,--fix"
    echo "        fix the format of source files"
    echo "    all,--all"
    echo "        check or fix the format of all source files in current directory"
    echo "    last,--last-commit"
    echo "        check or fix the format for the last commit of source files"
    echo "    dir,--directory"
    echo "        check or fix the format for source files by specify directory"
    echo "    files,--files"
    echo "        check or fix the format for specify source files"
    echo "    -h,--help"
    echo "        display this help"
    echo "EXAMPLES"
    echo "  use default clang-format path, which is export by environment variable CLANG_FORMAT_PATH"
    echo "    sh scripts/clang_format.sh check last"
    echo "    sh scripts/clang_format.sh check all"
    echo "    sh scripts/clang_format.sh check dir utils/src/container"
    echo "    sh scripts/clang_format.sh check files utils/src/container/dynamic_array.c"
    echo "    sh scripts/clang_format.sh fix last"
    echo "    sh scripts/clang_format.sh fix files utils/src/container/dynamic_array.c"
    echo ""
    echo "  specify clang-format path by parameter bin_path"
    echo "    sh scripts/clang_format.sh bin_path ./clang-format check dir utils/src/container"
}

function main()
{
    if [ $# -eq 0 ]; then
        show_help
        return 1
    fi

    # Check whether clang-format exists
    case "$1" in
        bin_path|--bin_path)
            CLANG_FORMAT=$2
            shift 2
            ;;
    esac
    is_clang_format_exist
    if [ $? -ne 0 ]; then
        return 1;
    fi

    # do check or fix by clang-format
    while [ $# -gt 0 ]; do
        case "$1" in
            check|--check)
                shift 1
                format_check $*
                return $has_format_error
                ;;
            fix|--fix)
                shift 1
                format_fix $*
                return 0
                ;;
            *|-h|--help)
                show_help
                return 1
                ;;
        esac
    done
}

main "$@"
exit $?
