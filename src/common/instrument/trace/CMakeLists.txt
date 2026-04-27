execute_process(
    COMMAND perl gentraceid -s ${CMAKE_CURRENT_SOURCE_DIR} -t ${PROJECT_SOURCE_DIR}/include/common/instrument/trace
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)
