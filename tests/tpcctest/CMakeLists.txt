set(CMAKE_SKIP_RPATH FALSE)

# Please make sure to only put cpp files in src folder
FILE(GLOB_RECURSE tpcctest_src_list RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)

set(tpcctest_compile_options ${PROTECT_OPTIONS} ${CHECK_OPTIONS} ${BIN_SECURE_OPTIONS})

set(tpcctest_link_options ${LIB_LINK_OPTIONS})

set(tpcctest_link_libs dstore securec lz4 cjson z gsutils utilities)

set(tpcctest_macro_options ${MACRO_OPTIONS})

set(tpcctest_include_directories
    ${PROJECT_SOURCE_DIR}/interface
    ${PROJECT_SOURCE_DIR}/include
    ${SECURE_INCLUDE_PATH}
    ${UTILS_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
    include
)

set(tpcctest_link_directories
    ${CJSON_LIB_PATH} ${SECURE_LIB_PATH}
    ${UTILS_LIB_PATH} ${LIBEVENT_LIB_PATH}
)

add_bintarget(tpcctest tpcctest_src_list tpcctest_include_directories "${tpcctest_macro_options}" "${tpcctest_compile_options}" "${tpcctest_link_options}" "${tpcctest_link_libs}")
target_link_directories(tpcctest PRIVATE ${tpcctest_link_directories})
add_dependencies(tpcctest dstore utilities)

set(VFSCONFIGPATH ${UTILS_LIB_PATH}/libvfslinuxadapter.so)

execute_process(COMMAND sed -i "s#\"clientLibPath\":\\s*\"[^\"]*\"#\"clientLibPath\": \"${VFSCONFIGPATH}\"#" ${CMAKE_CURRENT_SOURCE_DIR}/../../build_script/dstore_conf.json)

set(STARTCONFIGPATH ${CMAKE_CURRENT_SOURCE_DIR}/../../build_script/dstore_conf.json)

execute_process(COMMAND sed -i "s#\"startConfigPath\":\\s*\"[^\"]*\"#\"startConfigPath\": \"${STARTCONFIGPATH}\"#" ${CMAKE_CURRENT_SOURCE_DIR}/../../build_script/guc.json)

install(FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/config.json"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../build_script/guc.json"
    "${CMAKE_CURRENT_SOURCE_DIR}/../../build_script/dstore_conf.json"
    DESTINATION "${CMAKE_BINARY_DIR}"
)