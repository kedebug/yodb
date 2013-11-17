set(GSOURCE
    mutex_test.cc
    thread_test.cc
    node_test.cc
)

add_executable(test ${GSOURCE})
target_link_libraries(test yodb gtest_main gtest)

add_executable(logger logger_test.cc)
target_link_libraries(logger yodb)
