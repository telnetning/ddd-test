add_subdirectory(src)

if (${ENABLE_UT} STREQUAL "ON")
    enable_testing()
    add_subdirectory(tests)
endif()
