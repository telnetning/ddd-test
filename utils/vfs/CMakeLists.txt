add_subdirectory(src)

# unittest (Must have CAP_SYS_CHROOT capability to run vfs adapter unittest )
if (${ENABLE_UT} STREQUAL "ON")
    set(VFS_DYNAMIC_LIB_PATH "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/libvfslinuxadapter.so")
    message(STATUS "vfs linux adapter lib path: ${VFS_DYNAMIC_LIB_PATH}")
    add_definitions(-DLINUX_LIB_PATH="${VFS_DYNAMIC_LIB_PATH}")

    enable_testing()
    add_subdirectory(test)
endif()
