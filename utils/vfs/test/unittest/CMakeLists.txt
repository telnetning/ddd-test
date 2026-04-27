file(GLOB vfs_unittest_src_list
    ./ut_vfs/ut_vfs_adapter.cpp
)

set(vfs_unittest_compile_options ${PROTECT_OPTIONS} ${CHECK_OPTIONS} ${LIB_SECURE_OPTIONS})
set(vfs_unittest_link_options ${BIN_LINK_OPTIONS})
set(vfs_unittest_macro_options ${MACRO_OPTIONS})
set(vfs_unittest_include_directories
    ${GTEST_INCLUDE_PATH}
    ${MOCKCPP_INCLUDE_PATH}
    ${MOCKCPP_3RDPARTY_PATH}
    ${CMAKE_SOURCE_DIR}/utils/interface
    ${SECURE_INCLUDE_PATH}
)
set(vfs_unittest_link_directories
    ${GTEST_LIB_PATH}
    ${MOCKCPP_LIB_PATH}
    ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}
    ${SECURE_LIB_PATH}
)
set(vfs_unittest_link_libs
    pthread
    ${UT_LIBS}
    mockcpp
    gsutils
    securec
)

add_bintarget(vfs_unittest vfs_unittest_src_list vfs_unittest_include_directories "${vfs_unittest_macro_options}" "${vfs_unittest_compile_options}" "${vfs_unittest_link_options}" "${vfs_unittest_link_libs}")
target_link_directories(vfs_unittest BEFORE PRIVATE ${vfs_unittest_link_directories})
add_dependencies(vfs_unittest gsutils)

add_test(NAME vfs_unittest COMMAND vfs_unittest)

option(GRANT_CHROOT_CAPABILITY "Grant chroot capability to unittest program: OFF, ON" "OFF")
message(STATUS "Grant chroot capability to unittest program: ${GRANT_CHROOT_CAPABILITY}")
if (GRANT_CHROOT_CAPABILITY STREQUAL "ON")
    add_custom_command(
        TARGET vfs_unittest
        POST_BUILD
        COMMAND sudo setcap CAP_SYS_CHROOT+eip ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/vfs_unittest
        VERBATIM
    )
endif()

add_custom_target(run_vfs_unittest
    DEPENDS vfs_unittest
    COMMAND ${CMAKE_BINARY_DIR}/bin/vfs_unittest
    COMMENT "***************begin vfs unit test***************"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
)

set_property(GLOBAL APPEND PROPERTY all_utils_unittest_list "run_vfs_unittest")
