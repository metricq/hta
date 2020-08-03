add_executable(hta.test_basics basics.cpp)
add_test(hta.test_basics hta.test_basics)
target_link_libraries(hta.test_basics PRIVATE hta::hta catch::catch json::json)

add_executable(hta.test_retrieve retrieve.cpp)
add_test(hta.test_retrieve hta.test_retrieve)
target_link_libraries(hta.test_retrieve PRIVATE hta::hta catch::catch json::json)

add_executable(hta.test_count count.cpp)
add_test(hta.test_count hta.test_count)
target_link_libraries(hta.test_count PRIVATE hta::hta catch::catch json::json)

add_executable(hta.test_aggregate aggregate.cpp)
add_test(hta.test_aggregate hta.test_aggregate)
target_link_libraries(hta.test_aggregate PRIVATE hta::hta catch::catch json::json)


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

add_executable(hta.test_read_write read_write.cpp)
add_test(hta.test_read_write hta.test_read_write)
target_link_libraries(hta.test_read_write PRIVATE hta::hta catch::catch json::json)

add_executable(hta.test_prefix prefix.cpp)
add_test(hta.test_prefix hta.test_prefix)
target_link_libraries(hta.test_prefix PRIVATE hta::hta catch::catch json::json)

add_executable(hta.test_flex flex.cpp)
add_test(hta.test_flex hta.test_flex)
target_link_libraries(hta.test_flex PRIVATE hta::hta catch::catch json::json)

find_package(benchmark QUIET)
if (benchmark_FOUND)
    message(STATUS "Looking for google benchmark - found, will build benchmark tests.")

    add_executable(hta.bench bench.cpp)
    target_link_libraries(hta.bench PRIVATE hta::hta json::json benchmark::benchmark)

    if (USE_S3)
        add_executable(hta.bench_s3 bench_s3.cpp)
        target_link_libraries(hta.bench_s3 PRIVATE hta::hta json::json benchmark::benchmark ${AWSSDK_LINK_LIBRARIES})
    endif()
else ()
    message(STATUS "Looking for google benchmark - not found, skipping benchmark tests.")
endif ()
