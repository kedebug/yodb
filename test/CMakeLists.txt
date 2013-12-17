set(GSOURCE
    thread_test.cc
#    node_test.cc
    slice_test.cc
    block_test.cc
)

# add_executable(test ${GSOURCE})
# target_link_libraries(test yodb gtest_main gtest)
# 
# add_executable(logger logger_test.cc)
# target_link_libraries(logger yodb)
# 
# add_executable(mutex mutex_test.cc)
# target_link_libraries(mutex yodb)
# 
# add_executable(rwlock rwlock_test.cc)
# target_link_libraries(rwlock yodb)
# 
add_executable(dbimpl dbimpl_test.cc)
target_link_libraries(dbimpl yodb)
# 
# add_executable(file file_test.cc)
# target_link_libraries(file yodb)
# 
# add_executable(table table_test.cc)
# target_link_libraries(table yodb)

add_executable(db db_test.cc)
target_link_libraries(db yodb)

add_executable(skiplist skiplist_test.cc)
target_link_libraries(skiplist yodb)

add_executable(benchmark db_bench.cc histogram.cc testutil.cc)
target_link_libraries(benchmark yodb)
