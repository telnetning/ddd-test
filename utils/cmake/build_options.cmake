# configure CMake parameters
option(ENABLE_TCP_ONLY "enable tcp only" OFF)
option(ENABLE_UT "enable unitest" OFF)
option(ENABLE_FUZZ "enable fuzz" OFF)
option(ENABLE_PERF "enable perf" OFF)
option(ENABLE_LCOV "enable lcov coverage" OFF)
option(ENABLE_HCOV "enable hitest coverage" OFF)
option(ENABLE_OBJDIFF "enable objdiff" OFF)
option(SUPPORT_HOTPATCH "support hotpatch" OFF)
option(RET_ARM_HOTPATCH "ret arm hotpatch" OFF)

option(GOT_OPTIMIZE "GOT OPTIMIZE (hidden internal symbol): OFF, ON" "OFF")
message(STATUS "GOT_OPTIMIZE: ${GOT_OPTIMIZE}")

if(DEFINED ENV{DEBUG_TYPE})
    string(TOLOWER $ENV{DEBUG_TYPE} CMAKE_BUILD_TYPE_TMP)
endif()
if(CMAKE_BUILD_TYPE)
    string(TOLOWER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE_TMP)
endif()
set(CMAKE_BUILD_TYPE ${CMAKE_BUILD_TYPE_TMP})
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "debug" CACHE STRING "Default build type is debug" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "debug" "release")
endif()
message(STATUS "CMAKE_BUILD_TYPE ====== ${CMAKE_BUILD_TYPE}")

# Check CPU architecture
set(CPU_ARCH_TYPE "x86")
if (${CMAKE_HOST_SYSTEM_PROCESSOR} STREQUAL "aarch64")
    set(CPU_ARCH_TYPE "ARM")
endif()
message(STATUS "Utils build CPU architecture: ${CPU_ARCH_TYPE}")

set(PROTECT_OPTIONS -fwrapv -fnon-call-exceptions)
set(WARNING_OPTIONS -Wall -Werror -fstack-protector -Wendif-labels -Wformat-security -Wextra -Wtrampolines
                    -Wstrict-prototypes -Wdate-time -Wfloat-equal -Wswitch-default -Wshadow -Wcast-qual -Wcast-align
                    -Wvla -Wunused -Wundef -Wconversion -Wformat=2)
set(OPTIMIZE_OPTIONS -pipe -pthread -fsigned-char -fno-aggressive-loop-optimizations -fno-expensive-optimizations
                     -fno-omit-frame-pointer -fno-strict-aliasing -fno-common -freg-struct-return -fstrong-eval-order
                     -Wpointer-arith -fmerge-constants)
set(CHECK_OPTIONS -Wmissing-format-attribute -Wno-unused-but-set-variable)
set(MACRO_OPTIONS -D_GNU_SOURCE)

# TODO: remove
list(REMOVE_ITEM OPTIMIZE_OPTIONS -Wpointer-arith)
list(REMOVE_ITEM CHECK_OPTIONS -Wmissing-format-attribute)
# TODO: remove

# set options
if(CMAKE_BUILD_TYPE STREQUAL debug)
    list(APPEND PROTECT_OPTIONS -O0 -ggdb)
    list(APPEND MACRO_OPTIONS -DGSDB_DEBUG=1)
elseif(CMAKE_BUILD_TYPE STREQUAL release)
    set(SUPPORT_HOTPATCH ON)
    list(APPEND PROTECT_OPTIONS -O2 -g3)
elseif(CMAKE_BUILD_TYPE STREQUAL memcheck)
    list(APPEND PROTECT_OPTIONS -O0 -g)
    list(APPEND MACRO_OPTIONS -DGSDB_DEBUG=1)
elseif(CMAKE_BUILD_TYPE STREQUAL coverage)
    set(SUPPORT_HOTPATCH ON)
    list(APPEND PROTECT_OPTIONS -O2 -g3)
    set(ENABLE_LCOV ON)
else()
    list(APPEND PROTECT_OPTIONS -O0 -g)
endif()

# libraries need secure options during compling
set(LIB_SECURE_OPTIONS -fPIC -fstack-protector)
# libraries need link options during linking
set(LIB_LINK_OPTIONS -pthread -Wl,--build-id=uuid -Wl,-z,noexecstack -Wl,-z,relro,-z,now)

set(BIN_SECURE_OPTIONS -fstack-protector)
set(BIN_LINK_OPTIONS -pthread -Wl,--build-id=uuid -Wl,-z,noexecstack -Wl,-z,relro,-z,now)
if(${ENABLE_UT} STREQUAL "ON" OR ${ENABLE_LCOV} STREQUAL "ON")
    # UT test need change binaries to libraries,set -fPIC during compling
    list(APPEND BIN_SECURE_OPTIONS -fPIC)
    list(APPEND BIN_LINK_OPTIONS -fPIC)
    list(REMOVE_ITEM WARNING_OPTIONS -Werror)
else()
    # binaries need fPIE during compling
    list(APPEND BIN_SECURE_OPTIONS -fPIE)
    # binaries need fPIE pie link options during linking
    list(APPEND BIN_LINK_OPTIONS -pie)
endif()
if (NOT (${CMAKE_HOST_SYSTEM_PROCESSOR} STREQUAL "aarch64") )
    list(APPEND CHECK_OPTIONS -msse4.2)
endif()
set(MEMCHECK_LINK_LIBS "")
set(TEST_LINK_DIRECTORIES "")
# set memcheck compile options
if(CMAKE_BUILD_TYPE STREQUAL memcheck)
    list(APPEND CHECK_OPTIONS -fsanitize=address,leak,undefined -fno-omit-frame-pointer -fno-sanitize=vla-bound -fno-sanitize=nonnull-attribute -fno-sanitize=alignment -fsanitize-recover=undefined)
    list(APPEND LIB_LINK_OPTIONS -fsanitize=address,leak,undefined -fsanitize-recover=undefined)
    list(APPEND BIN_LINK_OPTIONS -fsanitize=address,leak,undefined -fsanitize-recover=undefined)
    list(REMOVE_ITEM LIB_SECURE_OPTIONS -fstack-protector)
    list(REMOVE_ITEM BIN_SECURE_OPTIONS -fstack-protector)
    list(APPEND MEMCHECK_LINK_LIBS -static-libasan -static-libubsan dl m rt)
    if("${GCC_VERSION}" STREQUAL "10.3.1")
        list(APPEND MEMCHECK_LINK_LIBS stdc++)
    endif()
    list(APPEND MACRO_OPTIONS -DENABLE_MEMORY_CHECK)
    list(APPEND TEST_LINK_DIRECTORIES ${MEMCHECK_LIB_PATH})
    list(REMOVE_ITEM WARNING_OPTIONS -Werror)
    list(REMOVE_ITEM WARNING_OPTIONS -fstack-protector)
endif()

set(TEST_LINK_LIBS "")
if(${ENABLE_LCOV} STREQUAL "ON")
    list(APPEND CHECK_OPTIONS -fprofile-arcs -ftest-coverage)
    list(APPEND TEST_LINK_LIBS -lgcov)
    add_definitions(-D ENABLE_LCOV)
    message(STATUS "Enable lcov coverage")
endif()

if(${ENABLE_HCOV} STREQUAL "ON")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE hitestwrapper)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK hitestwrapper)
    add_definitions(-D ENABLE_HCOV)
    message(STATUS "Enable hitest coverage")
endif()

if(${ENABLE_UT} STREQUAL "ON")
    set(UTILS_UT_TOOLS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/utils/tests/tools)
    list(APPEND MACRO_OPTIONS -DENABLE_UT=ON -DENABLE_FAULT_INJECTION=1)

    if(${CPU_ARCH_TYPE} STREQUAL "x86")
        list(APPEND MACRO_OPTIONS -D_GLIBCXX_USE_CXX11_ABI=0)
        # Compatible gtest with old interfaces
        set(UT_LIBS  gmock_main gtest gmock)
    else()
        set(UT_LIBS gmock_maind gtestd gmockd)
    endif()
    set_property(GLOBAL PROPERTY all_utils_unittest_list "")
endif()

#hotpatch
if(${CPU_ARCH_TYPE} STREQUAL "ARM")
    set(RET_ARM_HOTPATCH "ON")
endif()

#objdiff
if(${ENABLE_OBJDIFF} STREQUAL "ON")
    list(REMOVE_ITEM WARNING_OPTIONS -Werror)
    list(REMOVE_ITEM LIB_SECURE_OPTIONS -fPIC)
    list(APPEND CHECK_OPTIONS -ffunction-sections -D__LINE__=0 -fno-PIE -fPIC -Wa,--compress-debug-sections=none -fdata-sections -fno-section-anchors -Wa,-compress-debug-sections=none -fno-common)
    set(HOTPATCH_ATOMIC_LDS -Wl,-T${LIBHOTPATCH_TOOL_PATH}/atomic.lds)
    if(${CPU_ARCH_TYPE} STREQUAL "x86")
        list(APPEND CHECK_OPTIONS -mcmodel=large -Wa,-mrelax-relocations=no)
    endif()
endif()

# set __FILE__ to relative path
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-builtin-macro-redefined -D__FILE__='\"$(notdir $(subst .o,,$(abspath $@)))\"'")
