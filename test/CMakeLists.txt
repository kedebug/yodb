set(GSOURCE
    thread_test.cc
    node_test.cc
    slice_test.cc
)

add_executable(test ${GSOURCE})
target_link_libraries(test yodb gtest_main gtest)

add_executable(logger logger_test.cc)
target_link_libraries(logger yodb)

add_executable(mutex mutex_test.cc)
target_link_libraries(mutex yodb)

add_executable(rwlock rwlock_test.cc)
target_link_libraries(rwlock yodb)
