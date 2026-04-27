# see the macro definition in build_function.cmake
SUBDIRLIST(SUBDIRS ${CMAKE_CURRENT_SOURCE_DIR})

set(utils_fuzz_unittest_src_list "")
FOREACH (subdir ${SUBDIRS})
    # we are concatenating every directory's source files to ${utils_fuzz_unittest_src_list}
    AUX_SOURCE_DIRECTORY(${subdir} utils_fuzz_unittest_src_list)
ENDFOREACH()
list(APPEND utils_fuzz_unittest_src_list ${CMAKE_CURRENT_SOURCE_DIR}/utils_fuzz_gtest_main.cpp)

list(APPEND OPTIMIZE_OPTIONS -fsanitize-coverage=trace-pc -fno-plt -w -g)
set(utils_fuzz_unittest_compile_options ${PROTECT_OPTIONS} ${OPTIMIZE_OPTIONS} ${CHECK_OPTIONS} ${LIB_SECURE_OPTIONS})
set(utils_fuzz_unittest_link_options ${BIN_LINK_OPTIONS})

list(APPEND MACRO_OPTIONS -DCOMPILE_LIBRARY_DIR="${CMAKE_LIBRARY_OUTPUT_DIRECTORY}" -DUTILS_UT_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
list(APPEND MACRO_OPTIONS -DATOMIC_BUILTINS)
set(utils_fuzz_unittest_macro_options ${MACRO_OPTIONS})
# Add link directories for Fuzz
set(FUZZ_PATH ${3RD_PATH}/buildtools/secodefuzz)
set(utils_fuzz_unittest_include_directories
    ${FUZZ_PATH}
    ${GTEST_INCLUDE_PATH}
    ${MOCKCPP_INCLUDE_PATH}
    ${MOCKCPP_3RDPARTY_PATH}
    ${SECURE_INCLUDE_PATH}
    ${CMAKE_SOURCE_DIR}/utils/interface
    ${CMAKE_SOURCE_DIR}/utils/include
    ${UTILS_UT_TOOLS_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/communication/interface/
)
set(utils_fuzz_unittest_link_directories
    ${FUZZ_PATH}
    ${GTEST_LIB_PATH}
    ${MOCKCPP_LIB_PATH}
    ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
    ${SECURE_LIB_PATH}
)
set(utils_fuzz_unittest_link_libs
    pthread
    ${UT_LIBS}
    mockcpp
    gsutils
    securec
    rt
    secodefuzz
)

add_bintarget(utils_fuzz_unittest utils_fuzz_unittest_src_list utils_fuzz_unittest_include_directories "${utils_fuzz_unittest_macro_options}" "${utils_fuzz_unittest_compile_options}" "${utils_fuzz_unittest_link_options}" "${utils_fuzz_unittest_link_libs}")
target_link_directories(utils_fuzz_unittest BEFORE PRIVATE ${utils_fuzz_unittest_link_directories})
add_dependencies(utils_fuzz_unittest gsutils vfslinuxadapter)

add_test(NAME utils_fuzz_unittest COMMAND utils_fuzz_unittest)

option(GRANT_CHROOT_CAPABILITY "Grant chroot capability to unittest program: OFF, ON" "OFF")
message(STATUS "Grant chroot capability to unittest program: ${GRANT_CHROOT_CAPABILITY}")
if (GRANT_CHROOT_CAPABILITY STREQUAL "ON")
    add_custom_command(TARGET utils_fuzz_unittest
        POST_BUILD
        COMMAND sudo setcap CAP_SYS_CHROOT+eip ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/utils_fuzz_unittest
        VERBATIM
        )
endif()

add_custom_target(run_utils_fuzz_unittest
    DEPENDS utils_fuzz_unittest
    COMMAND ${CMAKE_BINARY_DIR}/bin/utils_fuzz_unittest
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

set_property(GLOBAL APPEND PROPERTY all_utils_unittest_list "run_utils_fuzz_unittest")
