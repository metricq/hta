add_executable(hta.test_basics basics.cpp)
add_test(hta.test_basics hta.test_basics)
target_link_libraries(hta.test_basics PRIVATE hta::hta catch::catch json::json)
