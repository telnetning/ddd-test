# generate_run_targets.cmake
set(TARGET_CONTENT "# Auto-generated test targets\n")

# Step 1: find all executable unittests
file(GLOB_RECURSE ALL_UNITTESTS RELATIVE ${INSTALLED_BIN_DIR}
    LIST_DIRECTORIES false
    ${INSTALLED_BIN_DIR}/*test
    ${INSTALLED_BIN_DIR}/*unittest
)

# Step 2: Clean up quotes if any
set(CLEANED_ALL_UNITTESTS)
foreach(item IN LISTS ALL_UNITTESTS)
    get_filename_component(filename "${item}" NAME)
    string(TOLOWER "${filename}" lower_filename)
    if(${lower_filename} MATCHES "tpcc")
        message(STATUS "Skip tpcc-related test: ${filename}")
        continue()
    endif()
    string(REPLACE "\"" "" clean_item "${item}")
    list(APPEND CLEANED_ALL_UNITTESTS "${clean_item}")
endforeach()
set(ALL_UNITTESTS ${CLEANED_ALL_UNITTESTS})

# Step 3: Collect valid test executables using nested if()
set(UNITTESTS)
foreach(EXE IN LISTS ALL_UNITTESTS)
    if(EXISTS "${EXE}")
        execute_process(
            COMMAND sh -c "[ -x \"${EXE}\" ]"
            RESULT_VARIABLE is_executable
        )
        if(is_executable EQUAL 0)
            list(APPEND UNITTESTS "${EXE}")
        endif()
    endif()
endforeach()

# Step 4: Output result
message("Found valid test executables: ${UNITTESTS}")

foreach(unittest ${UNITTESTS})
    get_filename_component(TESTNAME ${unittest} NAME_WE)
    message(STATUS "Processing test executable: ${TESTNAME}")

    # if no GROUP_STRATEGY or == 0 | no split
    if("${GROUP_STRATEGY}" STREQUAL "" OR "${GROUP_STRATEGY}" EQUAL 0)
        string(APPEND TARGET_CONTENT "
add_custom_target(run_${TESTNAME}_test
    COMMAND \"${unittest}\"
    COMMENT \"****** Running full test suite for ${TESTNAME} ******\"
)\n")
    else()
        execute_process(
            COMMAND ${unittest} --gtest_list_tests
            OUTPUT_VARIABLE OUTPUT_TESTLIST
            ERROR_QUIET
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )

        string(FIND "${OUTPUT_TESTLIST}" "Test." test_pos)
        if(NOT test_pos EQUAL -1)
            set(pos ${test_pos})
            while(pos GREATER 0)
                string(SUBSTRING "${OUTPUT_TESTLIST}" ${pos} 1 char)
                if(char STREQUAL "\n")
                    break()
                endif()
                math(EXPR pos "${pos} - 1")
            endwhile()

            if(char STREQUAL "\n")
                math(EXPR pos "${pos} + 1")
            endif()

            string(SUBSTRING "${OUTPUT_TESTLIST}" ${pos} -1 TESTLIST)
        else()
            string(REGEX REPLACE "\n" ";" TESTLIST "${OUTPUT_TESTLIST}")
            list(FILTER TESTLIST EXCLUDE REGEX "^Running[ ]+main\$\$.*main\\.cc\$")
        endif()

        string(REGEX REPLACE "\n" ";" TESTLIST "${TESTLIST}")
        list(FILTER TESTLIST INCLUDE REGEX "^[A-Za-z0-9_]+\\.")

        if(NOT TESTLIST)
            message("No valid test suites found in ${TESTNAME}")
            continue()
        endif()

        set(group_index 0)
        set(current_group "")

        foreach(suite ${TESTLIST})
            list(APPEND current_group ${suite})

            list(LENGTH current_group current_group_len)
            if(${current_group_len} EQUAL ${GROUP_STRATEGY})
                set(filter "")
                foreach(s ${current_group})
                    if(filter STREQUAL "")
                        set(filter "--gtest_filter=${s}*")
                    else()
                        set(filter "${filter}:${s}*")
                    endif()
                endforeach()

                string(APPEND TARGET_CONTENT "
add_custom_target(run_${TESTNAME}_group${group_index}
    COMMAND \"${unittest}\" ${filter}
    COMMENT \"Running group ${group_index} ${filter} of ${TESTNAME}\"
)\n")

                set(current_group "")
                math(EXPR group_index "${group_index} + 1")
            endif()
        endforeach()

        # last group
        if(NOT "${current_group}" STREQUAL "")
            set(filter "")
            foreach(s ${current_group})
                if(filter STREQUAL "")
                    set(filter "--gtest_filter=${s}*")
                else()
                    set(filter "${filter}:${s}*")
                endif()
            endforeach()

            string(APPEND TARGET_CONTENT "
add_custom_target(run_${TESTNAME}_group${group_index}
    COMMAND \"${unittest}\" ${filter}
    COMMENT \"****** Running final group ${group_index} ${filter} of ${TESTNAME} ******\"
)\n")
        endif()
    endif()
endforeach()

file(WRITE ${RUN_TARGETS_SCRIPT} "${TARGET_CONTENT}")
