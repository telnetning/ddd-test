set(LOCAL_LIB_PATH $ENV{LOCAL_LIB_PATH} CACHE PATH "Local lib root path")
option(ENABLE_UT "enable ut, current value is --enable-ut" OFF)
set(LIB_UNIFIED_SUPPORT comm)

############################################################################
# secure
############################################################################
set(SECURE_HOME ${LOCAL_LIB_PATH}/secure)
set(SECURE_INCLUDE_PATH ${SECURE_HOME}/include)
set(SECURE_LIB_PATH ${SECURE_HOME}/lib)

############################################################################
# cjson
############################################################################
set(CJSON_INCLUDE_PATH ${LOCAL_LIB_PATH}/cjson/include)
set(CJSON_LIB_PATH ${LOCAL_LIB_PATH}/cjson/lib)

############################################################################
# gtest
############################################################################
set(GTEST_HOME ${LOCAL_LIB_PATH}/gtest)
set(GTEST_INCLUDE_PATH ${GTEST_HOME}/include)
set(GTEST_LIB_PATH ${GTEST_HOME}/lib)

#############################################################################
# coverage
#############################################################################
set(LCOV_HOME ${LOCAL_LIB_PATH}/buildtools/gcc${GCC_VERSION_LIT}/gcc/lib/gcc/${HOST_TUPLE})
set(LCOV_LIB_PATH ${LCOV_HOME}/${GCC_VERSION})