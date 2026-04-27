if (${ENABLE_FUZZ} STREQUAL "ON")
    add_subdirectory(fuzz_test)
    message(STATUS "fuzz test is setted")
endif()

# see the macro definition in build_function.cmake
SUBDIRLIST(SUBDIRS ${CMAKE_CURRENT_SOURCE_DIR})
# add the directories you want to sift out from the whole UT suite
set(ut_filter_list "ut_multi_node_framework" "ut_copy_file")
# pick the fuzz test directories
set(ut_fuzz_list "fuzz_test")

set(utils_unittest_src_list "")
FOREACH (subdir ${SUBDIRS})
    if ("${subdir}" IN_LIST ut_filter_list)
        message(STATUS "UT Tests ignore the dir: ${subdir}")
        continue()
    endif()
    if ("${subdir}" IN_LIST ut_fuzz_list)
        continue()
    endif()
    # we are concatenating every directory's source files to ${utils_unittest_src_list}
    AUX_SOURCE_DIRECTORY(${subdir} utils_unittest_src_list)
ENDFOREACH()
list(APPEND utils_unittest_src_list ${CMAKE_CURRENT_SOURCE_DIR}/utils_gtest_main.cpp)

list(APPEND OPTIMIZE_OPTIONS -fno-plt -w -g)
set(utils_unittest_compile_options ${PROTECT_OPTIONS} ${OPTIMIZE_OPTIONS} ${CHECK_OPTIONS} ${LIB_SECURE_OPTIONS})
set(utils_unittest_link_options ${BIN_LINK_OPTIONS})

list(APPEND MACRO_OPTIONS -DCOMPILE_LIBRARY_DIR="${CMAKE_LIBRARY_OUTPUT_DIRECTORY}" -DUTILS_UT_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
list(APPEND MACRO_OPTIONS -DATOMIC_BUILTINS)
set(utils_unittest_macro_options ${MACRO_OPTIONS})
set(utils_unittest_include_directories
    ${GTEST_INCLUDE_PATH}
    ${MOCKCPP_INCLUDE_PATH}
    ${MOCKCPP_3RDPARTY_PATH}
    ${SECURE_INCLUDE_PATH}
    ${CMAKE_SOURCE_DIR}/utils/interface
    ${CMAKE_SOURCE_DIR}/utils/include
    ${UTILS_UT_TOOLS_DIR}
    ${CMAKE_SOURCE_DIR}/communication/interface/
)
set(utils_unittest_link_directories
    ${GTEST_LIB_PATH}
    ${MOCKCPP_LIB_PATH}
    ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
    ${SECURE_LIB_PATH}
    ${OPENSSL_LIB_PATH}
)
set(utils_unittest_link_libs
    pthread
    ${UT_LIBS}
    mockcpp
    gsutils
    securec
    rt
)

add_bintarget(utils_unittest utils_unittest_src_list utils_unittest_include_directories "${utils_unittest_macro_options}" "${utils_unittest_compile_options}" "${utils_unittest_link_options}" "${utils_unittest_link_libs}")
target_link_directories(utils_unittest BEFORE PRIVATE ${utils_unittest_link_directories})
add_dependencies(utils_unittest gsutils vfslinuxadapter)

add_test(NAME utils_unittest COMMAND utils_unittest)

option(GRANT_CHROOT_CAPABILITY "Grant chroot capability to unittest program: OFF, ON" "OFF")
message(STATUS "Grant chroot capability to unittest program: ${GRANT_CHROOT_CAPABILITY}")
if (GRANT_CHROOT_CAPABILITY STREQUAL "ON")
    add_custom_command(
        TARGET utils_unittest
        POST_BUILD
        COMMAND sudo setcap CAP_SYS_CHROOT+eip ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/utils_unittest
        VERBATIM
    )
endif()

add_custom_target(run_utils_unittest
    DEPENDS utils_unittest
    COMMAND ${CMAKE_BINARY_DIR}/bin/utils_unittest
    COMMAND bash -x ${CMAKE_SOURCE_DIR}/utils/tests/unittest/analyse_ut_log.sh ${CMAKE_BINARY_DIR}
    COMMENT "*************** begin utils unit test ***************"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

set_property(GLOBAL APPEND PROPERTY all_utils_unittest_list "run_utils_unittest")
