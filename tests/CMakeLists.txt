add_executable(hta.test_basics basics.cpp)
add_test(hta.test_basics hta.test_basics)
target_link_libraries(hta.test_basics PRIVATE hta::hta catch::catch json::json)

add_executable(hta.test_aggregation aggregation.cpp)
add_test(hta.test_aggregation hta.test_aggregation)
target_link_libraries(hta.test_aggregation PRIVATE hta::hta catch::catch json::json)

add_executable(hta.test_restore restore.cpp)
add_test(hta.test_restore hta.test_restore)
target_link_libraries(hta.test_restore PRIVATE hta::hta catch::catch json::json)

add_executable(hta.test_scope scope.cpp)
add_test(hta.test_scope hta.test_scope)
target_link_libraries(hta.test_scope PRIVATE hta::hta catch::catch json::json)

add_executable(hta.test_meta meta.cpp)
add_test(hta.test_meta hta.test_meta)
target_link_libraries(hta.test_meta PRIVATE hta::hta catch::catch json::json)

add_executable(hta.test_middle_interval middle_interval.cpp)
add_test(hta.test_middle_interval hta.test_middle_interval)
target_link_libraries(hta.test_middle_interval PRIVATE hta::hta catch::catch json::json)

add_executable(hta.test_overflow overflow.cpp)
add_test(hta.test_overflow hta.test_overflow)
target_link_libraries(hta.test_overflow PRIVATE hta::hta catch::catch json::json)

add_executable(hta.test_read_write_conversion read_write_conversion.cpp)
add_test(hta.test_read_write_conversion hta.test_read_write_conversion)
target_link_libraries(hta.test_read_write_conversion PRIVATE hta::hta catch::catch json::json)

find_package(benchmark QUIET)
if (benchmark_FOUND)
    message(STATUS "Looking for google benchmark - found, will build benchmark tests.")

    add_executable(hta.bench bench.cpp)
    target_link_libraries(hta.bench PRIVATE hta::hta json::json benchmark::benchmark)
else()
    message(STATUS "Looking for google benchmark - not found, skipping benchmark tests.")
endif()
