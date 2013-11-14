set(GSOURCE
    mutex_test.cc
    thread_test.cc
)

add_executable(gdbtest ${GSOURCE})
target_link_libraries(gdbtest yodb gtest_main gtest)

add_executable(logger.test logger_test.cc)
target_link_libraries(logger.test yodb)
