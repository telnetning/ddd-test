cmake_minimum_required(VERSION 3.14)
project(utils)

set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)

# Set library and binary output directory
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

get_filename_component(UTILS_PARENT_DIR ${CMAKE_CURRENT_SOURCE_DIR} DIRECTORY)
get_filename_component(LOCAL_LIB_ROOT_DIR ${UTILS_PARENT_DIR} DIRECTORY)
set(DEFAULT_LOCAL_LIB_PATH ${LOCAL_LIB_ROOT_DIR}/local_libs)

if (DEFINED LOCAL_LIB_PATH)
    set(ENV{LOCAL_LIB_PATH} ${LOCAL_LIB_PATH})
else()
    set(LOCAL_LIB_PATH ${DEFAULT_LOCAL_LIB_PATH})
    set(ENV{LOCAL_LIB_PATH} ${DEFAULT_LOCAL_LIB_PATH})
endif()
set(LOCAL_LIB_PATH $ENV{LOCAL_LIB_PATH})

if (NOT EXISTS ${LOCAL_LIB_PATH})
    message(FATAL_ERROR "Local lib path ${LOCAL_LIB_PATH} not exists! current: ${DEFAULT_LOCAL_LIB_PATH}")
endif()

if("$ENV{GCC_VERSION}" STREQUAL "")
    execute_process(COMMAND gcc --version OUTPUT_VARIABLE GCC_VERSION_INFO OUTPUT_STRIP_TRAILING_WHITESPACE)
    string(REPLACE "\n" " " GCC_VERSION_INFO_TMP ${GCC_VERSION_INFO})
    string(REPLACE " " ";" GCC_VERSION_INFO_LIST ${GCC_VERSION_INFO_TMP})
    list(GET GCC_VERSION_INFO_LIST 2 GCC_VERSION)
else()
    set(GCC_VERSION $ENV{GCC_VERSION})
endif()
if("${GCC_VERSION}" STREQUAL "7.3.0")
    set(GCC_VERSION_LIT 7.3)
endif()

# utils common output path
set(UTILS_COMMON_DEFAULT_OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/output)
if((DEFINED CMAKE_INSTALL_PREFIX))
    if(${CMAKE_INSTALL_PREFIX} STREQUAL /usr/local)
        set(CMAKE_INSTALL_PREFIX ${UTILS_COMMON_DEFAULT_OUTPUT} CACHE STRING "install prefix" FORCE)
    endif()
else()
    set(CMAKE_INSTALL_PREFIX ${UTILS_COMMON_DEFAULT_OUTPUT} CACHE STRING "install prefix" FORCE)
endif()

message(STATUS "install prefix : ${CMAKE_INSTALL_PREFIX}")
set(CMAKE_INSTALL_PUBLIC_HEAD_DIR ${CMAKE_INSTALL_PREFIX}/include)

set(UT_MULTI_NODE_FRAMEWORK_DIR ${CMAKE_SOURCE_DIR}/utils/tests/unittest/ut_multi_node_framework)
set(UT_MULTI_NODE_FRAMEWORK_SRC 
    "${UT_MULTI_NODE_FRAMEWORK_DIR}/main.cpp"
    "${UT_MULTI_NODE_FRAMEWORK_SRC}/ut_multi_node_framework.cpp"
)

# Call cmake scripts
set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake/ CACHE INTERNAL "")
include(build_function)
include(set_lib_path)
include(build_options)
add_subdirectory(utils)
add_subdirectory(vfs)

if (CMAKE_BUILD_TYPE STREQUAL "memcheck")
    file(GLOB libasan_files ${MEMCHECK_LIB_PATH}/libasan.so*)
    file(GLOB libubsan_files ${MEMCHECK_LIB_PATH}/libubsan.so*)
    set(memcheck_libs ${libasan_files} ${libubsan_files})

    install(FILES ${memcheck_libs} DESTINATION lib
        PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ
    )
endif()

if (${ENABLE_UT} STREQUAL "ON")
    get_property(all_utils_unittest_list GLOBAL PROPERTY all_utils_unittest_list)
    message("all utils unit test project list: ${all_utils_unittest_list}")
    add_custom_target(all_utils_unittest
        DEPENDS ${all_utils_unittest_list}
        COMMAND bash -x ${CMAKE_SOURCE_DIR}/utils/tests/unittest/analyse_ut_log.sh ${CMAKE_BINARY_DIR}
        COMMENT "end all unit test."
    )
endif()

execute_process(COMMAND rm -rf ${CMAKE_BINARY_DIR}/graph)
execute_process(COMMAND mkdir ${CMAKE_BINARY_DIR}/graph)
add_custom_target(graphviz
    COMMAND rm -rf ${CMAKE_BINARY_DIR}/graph/*
    COMMAND "${CMAKE_COMMAND}" "--graphviz=graph/utils_graph" .
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
)

if (${ENABLE_UT} STREQUAL "ON")
    set(RUN_TARGETS_SCRIPT "${CMAKE_BINARY_DIR}/run_targets.cmake")
    set(FIRST_CONFIGURE_RUN_ONCE "${CMAKE_BINARY_DIR}/first_configure_run_once.stamp")

    if(NOT EXISTS "${FIRST_CONFIGURE_RUN_ONCE}")
        message(STATUS "First-time configure: creating empty run_targets.cmake")
        configure_file(
            ${CMAKE_MODULE_PATH}/run_targets.in.cmake
            ${RUN_TARGETS_SCRIPT}
            COPYONLY
        )
        file(WRITE "${FIRST_CONFIGURE_RUN_ONCE}" "This file ensures we don't overwrite run_targets.cmake on reconfigure.")
    endif()

    include(${RUN_TARGETS_SCRIPT})

    install(CODE "
        message(STATUS \"Install completed, generating test targets...\")
        set(INSTALLED_BIN_DIR \"${CMAKE_BINARY_DIR}\")
        set(GROUP_STRATEGY ${GROUP_STRATEGY})
        set(RUN_TARGETS_SCRIPT \"${RUN_TARGETS_SCRIPT}\")
        include(\"${CMAKE_MODULE_PATH}/generate_run_targets.cmake\")
    ")
endif()