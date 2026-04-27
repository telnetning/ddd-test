# Please make sure to only put cpp files in src folder
FILE(GLOB_RECURSE pagedump_src_list RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)

list(APPEND OPTIMIZE_OPTIONS -Wl,-Dsymbolic)
set(pagedump_compile_options ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${OPTIMIZE_OPTIONS} ${CHECK_OPTIONS} ${BIN_SECURE_OPTIONS})
set(pagedump_link_options ${BIN_LINK_OPTIONS})
set(pagedump_link_libs dstore securec lz4 cjson z gsutils)
set(pagedump_macro_options ${MACRO_OPTIONS})
set(pagedump_include_directories
    ${PROJECT_SOURCE_DIR}/interface
    ${PROJECT_SOURCE_DIR}/include
    ${SECURE_INCLUDE_PATH}
    ${UTILS_INCLUDE_PATH}
    ${XGBOOST_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
    include
)
set(pagedump_link_directories
    ${CJSON_LIB_PATH} ${SECURE_LIB_PATH}
    ${UTILS_LIB_PATH} ${LIBEVENT_LIB_PATH}
)

add_bintarget(pagedump pagedump_src_list pagedump_include_directories "${pagedump_macro_options}" "${pagedump_compile_options}" "${pagedump_link_options}" "${pagedump_link_libs}")
target_link_directories(pagedump PRIVATE ${pagedump_link_directories})
add_dependencies(pagedump dstore)
