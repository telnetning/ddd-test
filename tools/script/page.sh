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

# Color variables
WHITE='\033[0;37m'
RED='\033[0;31m'
BLUE='\033[0;34m'

# Input parameter control
# Check number of input parameters
if [ $# -ne 4 ]
  then
    echo -e "${RED}Need 4 input parameters"
    echo "First parameter: relative directory_path that contain .log file"
    echo "Second parameter: pdbId"
    echo "Third parameter: m_fileId"
    echo -e "Fourth parameter: m_blockId${WHITE}"
    exit 1
fi

directory_path=$1
temp_file_path="$directory_path/temp.log"
input_file_path="$directory_path/*.log*"
output_file_path="$directory_path/result.log"
buftag="BufTag:($2, $3, $4)"
echo -e "${BLUE}Log file directories: $directory_path, $buftag${WHITE}"

# Check directory existance
if [ ! -d "$directory_path" ]
  then
    echo -e "${RED}Directory $directory_path doesn't exist"
    echo "The directory_path is RELATIVE to current directory"
    echo "..:Previous directory"
    echo -e ".: Current Directory${WHITE}"
fi

# Remove previous result.log file
if [ -e "$output_file_path" ]
  then
    rm $output_file_path
fi

# Count number of log files
count=$(ls -lt $input_file_path | wc -l)

# Check log file existance
if [ $count -eq 0 ]
  then
    echo "There are no .log files in directory $directory_path"
    exit 1
fi

# Concatenate all .log files into one file
cat $input_file_path > $temp_file_path

# Grep page info
grep "$buftag" $temp_file_path > $output_file_path

# Sort according to numerical value
# Sort third field which is the time numerically
# Problems could occur when the batch script is run midnight at 12:00AM
# As time jumps from 24:00 to 0:00
sort -Vk3,3 -o $output_file_path $output_file_path

# Remove generated temp.log file
if [ -e "$directory_path/temp.log" ]
  then
    rm $directory_path/temp.log
fi