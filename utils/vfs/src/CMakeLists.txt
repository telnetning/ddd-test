set(vfslinuxadapter_src_list
    vfs_linux_lib.c
)

set(vfslinuxadapter_compile_options ${PROTECT_OPTIONS} ${WARNING_OPTIONS} ${OPTIMIZE_OPTIONS} ${CHECK_OPTIONS} ${LIB_SECURE_OPTIONS})
set(vfslinuxadapter_link_options ${LIB_LINK_OPTIONS})
set(vfslinuxadapter_macro_options ${MACRO_OPTIONS})
set(vfslinuxadapter_include_directories
    ${CMAKE_SOURCE_DIR}/vfs/include
    ${CMAKE_SOURCE_DIR}/utils/interface
    ${CMAKE_SOURCE_DIR}/utils/include
    ${SECURE_INCLUDE_PATH}
)
set(vfslinuxadapter_link_directories
    ${SECURE_LIB_PATH}
)
set(vfslinuxadapter_link_libs
    securec
)

add_shared_libtarget(vfslinuxadapter vfslinuxadapter_src_list vfslinuxadapter_include_directories "${vfslinuxadapter_macro_options}" "${vfslinuxadapter_compile_options}" "${vfslinuxadapter_link_options}" "${vfslinuxadapter_link_libs}")
target_link_directories(vfslinuxadapter PUBLIC ${vfslinuxadapter_link_directories})

# install
install(
    TARGETS vfslinuxadapter
    EXPORT vfslinuxadapter
    RUNTIME DESTINATION ${CMAKE_INSTALL_PREFIX}/bin
    LIBRARY DESTINATION ${CMAKE_INSTALL_PREFIX}/lib
)
