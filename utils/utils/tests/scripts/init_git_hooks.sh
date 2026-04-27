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

# Init hooks for git

BASE_DIR=$(realpath "$(dirname "${BASH_SOURCE[0]}")")

cp ${BASE_DIR}/utils/tests/git-hooks-pre-commit ${BASE_DIR}/../.git/hooks/pre-commit
chmod +x ${BASE_DIR}/../.git/hooks/pre-commit
