set(CMAKE_SKIP_RPATH FALSE)

# Please make sure to only put cpp files in src folder
FILE(GLOB_RECURSE utilities_src_list RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

set(utilities_compile_options ${PROTECT_OPTIONS} ${CHECK_OPTIONS} ${LIB_SECURE_OPTIONS})

set(utilities_link_options ${LIB_LINK_OPTIONS})
set(utilities_link_libs dstore securec lz4 cjson z gsutils)
set(utilities_macro_options ${MACRO_OPTIONS})
set(utilities_include_directories
    ${PROJECT_SOURCE_DIR}/interface
    ${PROJECT_SOURCE_DIR}/include
    ${SECURE_INCLUDE_PATH}
    ${UTILS_INCLUDE_PATH}
    ${CJSON_INCLUDE_PATH}
    include
)
set(utilities_link_directories
    ${SECURE_LIB_PATH}
    ${UTILS_LIB_PATH} ${LIBEVENT_LIB_PATH}
)

add_shared_libtarget(utilities utilities_src_list utilities_include_directories "${utilities_macro_options}" "${utilities_compile_options}" "${utilities_link_options}" "${utilities_link_libs}")
target_link_directories(utilities PRIVATE ${utilities_link_directories})
add_dependencies(utilities dstore)
