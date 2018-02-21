add_executable(hta.test_basics basics.cpp)
add_test(hta.test_basics hta.test_basics)
target_link_libraries(hta.test_basics PRIVATE hta::hta catch::catch json::json)

add_executable(hta.test_aggregation aggregation.cpp)
add_test(hta.test_aggregation hta.test_aggregation)
target_link_libraries(hta.test_aggregation PRIVATE hta::hta catch::catch json::json)

add_executable(hta.test_restore restore.cpp)
add_test(hta.test_restore hta.test_restore)
target_link_libraries(hta.test_restore PRIVATE hta::hta catch::catch json::json)
