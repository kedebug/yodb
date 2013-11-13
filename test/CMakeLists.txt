file(GLOB_RECURSE SOURCE 
    "${CMAKE_CURRENT_SOURCE_DIR}",
    "*.cc"
)

add_executable(test ${SOURCE})
target_link_libraries(test yodb gtest_main gtest)
