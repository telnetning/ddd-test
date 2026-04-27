# Set all function symbol to hidden
if (${GOT_OPTIMIZE} STREQUAL "ON")
    set(CMAKE_C_VISIBILITY_PRESET hidden)
endif()

if (WIN32)
    set(PLATFORM win32)
else()
    set(PLATFORM posix)
endif()

# add all the source files
file(GLOB_RECURSE gsutils_src_list "${CMAKE_SOURCE_DIR}/utils/src/*/*.c" "${CMAKE_SOURCE_DIR}/utils/interface/*/*.h")
# Exclude source files that depend on the platform.
list(FILTER gsutils_src_list EXCLUDE REGEX "port/")
# Obtaining the Source Files of Dependent Platforms to be Added.
file(GLOB_RECURSE port_files_list "${CMAKE_SOURCE_DIR}/utils/src/port/${PLATFORM}_*.c" "${CMAKE_SOURCE_DIR}/utils/src/port/*_common.c"  "${CMAKE_SOURCE_DIR}/utils/interface/port/${PLATFORM}_*.h")
# Add all source files.
list(APPEND gsutils_src_list ${port_files_list})

# TODO: remove
list(REMOVE_ITEM WARNING_OPTIONS -Wformat=2)
# TODO: remove
list(APPEND WARNING_OPTIONS -Wstack-usage=8388608 -Wframe-larger-than=8388608)

set(gsutils_compile_options ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${OPTIMIZE_OPTIONS} ${CHECK_OPTIONS} ${LIB_SECURE_OPTIONS})

set(gsutils_link_options ${LIB_LINK_OPTIONS})
set(gsutils_link_libs
    pthread
    securec
    aio
    dl
    cjson
    z
    m
)

list(APPEND MACRO_OPTIONS -DATOMIC_BUILTINS)
set(gsutils_macro_options ${MACRO_OPTIONS})
set(gsutils_include_directories
    ${CMAKE_SOURCE_DIR}/utils/interface
    ${CMAKE_SOURCE_DIR}/utils/include
    ${SECURE_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
    ${SSL_INCLUDE_PATH}
)
set(gsutils_link_directories
    ${CJSON_LIB_PATH}
    ${SECURE_LIB_PATH}
    ${SSL_LIB_PATH}
)

if(${ENABLE_LCOV} STREQUAL "ON")
    list(APPEND gsutils_link_directories ${LCOV_LIB_PATH})
    list(APPEND gsutils_link_libs gcov)
endif()

add_shared_libtarget(gsutils gsutils_src_list gsutils_include_directories "${gsutils_macro_options}" "${gsutils_compile_options}" "${gsutils_link_options}" "${gsutils_link_libs}")
target_link_directories(gsutils BEFORE PUBLIC ${gsutils_link_directories})

# install
install(
    TARGETS gsutils
    EXPORT gsutils
    RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}/bin
    LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
)

SUBDIRLIST(INTERFACE_DIR_LIST ${CMAKE_SOURCE_DIR}/utils/interface)
foreach (interface_dir ${INTERFACE_DIR_LIST})
    file(GLOB current_head_files "${CMAKE_SOURCE_DIR}/utils/interface/${interface_dir}/*.h")
    install(
        FILES ${current_head_files}
        DESTINATION ${CMAKE_INSTALL_PUBLIC_HEAD_DIR}/${interface_dir}
    )
endforeach ()
