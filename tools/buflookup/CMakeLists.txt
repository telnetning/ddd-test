# Please make sure to only put cpp files in src folder
FILE(GLOB_RECURSE buflookup_src_list RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)

set(buflookup_compile_options ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${OPTIMIZE_OPTIONS} ${CHECK_OPTIONS} ${BIN_SECURE_OPTIONS})
set(buflookup_link_options ${BIN_LINK_OPTIONS})
set(buflookup_link_libs dstore securec lz4 cjson z gsutils dl)
set(buflookup_macro_options ${MACRO_OPTIONS})
set(buflookup_include_directories
    ${PROJECT_SOURCE_DIR}/interface
    ${PROJECT_SOURCE_DIR}/include
    ${SECURE_INCLUDE_PATH}
    ${UTILS_INCLUDE_PATH}
    ${XGBOOST_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
    include
)
set(buflookup_link_directories
    ${CJSON_LIB_PATH} ${SECURE_LIB_PATH}
    ${UTILS_LIB_PATH} ${LIBEVENT_LIB_PATH}
)

add_bintarget(buflookup buflookup_src_list buflookup_include_directories "${buflookup_macro_options}" "${buflookup_compile_options}" "${buflookup_link_options}" "${buflookup_link_libs}")
target_link_directories(buflookup PRIVATE ${buflookup_link_directories})
add_dependencies(buflookup dstore)
