project(hta)

cmake_minimum_required(VERSION 3.8)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")

include(cmake/DefaultBuildType.cmake)

include(CheckCXXSymbolExists)

add_library(std::filesystem INTERFACE IMPORTED)
set(CMAKE_REQUIRED_FLAGS "-std=c++17")
CHECK_CXX_SYMBOL_EXISTS(std::filesystem::status_known filesystem HAS_STD_FILESYSTEM)
set(CMAKE_REQUIRED_LIBRARIES "stdc++fs")
CHECK_CXX_SYMBOL_EXISTS(std::experimental::filesystem::status_known experimental/filesystem HAS_EXPERIMENTAL_STD_FILESYSTEM)
unset(CMAKE_REQUIRED_LIBRARIES)
unset(CMAKE_REQUIRED_FLAGS)
if(NOT HAS_STD_FILESYSTEM)
    if(NOT HAS_EXPERIMENTAL_STD_FILESYSTEM)
        message(SEND_ERROR "Cannot find std::filesystem, but it is required. Please use a fully C++17 compliant compiler.")
    else()
        set_target_properties(std::filesystem PROPERTIES INTERFACE_COMPILE_DEFINITIONS HAS_EXPERIMENTAL_STD_FILESYSTEM)
        # libstdc++ needs an extra lib for filesystem
        set_target_properties(std::filesystem PROPERTIES INTERFACE_LINK_LIBRARIES stdc++fs)
    endif()
else()
    set_target_properties(std::filesystem PROPERTIES INTERFACE_COMPILE_DEFINITIONS HAS_STD_FILESYSTEM)
    # libstdc++ needs an extra lib for filesystem
    set_target_properties(std::filesystem PROPERTIES INTERFACE_LINK_LIBRARIES stdc++fs)
endif()

add_subdirectory(lib/catch)
add_subdirectory(lib/json)

set(hta_VERSION 0.0)

set(SRCS
    src/aggregate.cpp src/directory.cpp
    src/base_metric.cpp src/read_metric.cpp src/write_metric.cpp src/read_write_metric.cpp
    src/variant_metric.cpp
    src/meta.cpp
    src/storage/file/directory.cpp src/storage/file/metric.cpp
    )

add_library(hta ${SRCS})
target_include_directories(hta
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(hta
    PUBLIC
        json::json
        std::filesystem
)

target_compile_features(hta PUBLIC cxx_std_17)

target_compile_options(hta
    PRIVATE
        -Wall -Wextra -pedantic -Wsign-compare
)

add_library(hta::hta ALIAS hta)

find_package(Boost COMPONENTS program_options system timer)

if(Boost_FOUND)
    add_executable(hta_check src/tools/check.cpp)
    target_link_libraries(hta_check PRIVATE hta::hta Boost::program_options Boost::system)

    add_executable(hta_dummy src/tools/dummy.cpp)
    target_link_libraries(hta_dummy PRIVATE hta::hta)

    add_executable(hta_dump src/tools/dump.cpp)
    target_link_libraries(hta_dump PRIVATE hta::hta Boost::program_options Boost::system)

    add_executable(hta_repair src/tools/repair.cpp)
    target_link_libraries(hta_repair PRIVATE hta::hta)
else()
    message(STATUS "Will not build hta_dummy, hta_dump, hta_repair missing boost")
endif()

find_package(MySQLConnectorCPP)
if(MYSQLCONNECTORCPP_FOUND AND Boost_FOUND)
    add_executable(hta_mysql_import src/tools/mysql_import.cpp)
    target_link_libraries(hta_mysql_import PRIVATE hta::hta ${MYSQLCONNECTORCPP_LIBRARIES}
            Boost::program_options Boost::system Boost::timer)
    target_include_directories(hta_mysql_import PRIVATE ${MYSQLCONNECTORCPP_INCLUDE_DIRS})
else()
    message(STATUS "Will not build hta_mysql_import, missing boost and/or mysql-connector-c++")
endif()

if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    target_compile_options(hta
        PUBLIC
            -Wall -Wextra -pedantic -Wsign-compare -Werror
    )

    install(DIRECTORY
        ${CMAKE_CURRENT_SOURCE_DIR}/include/hta
        DESTINATION include
    )
    install(TARGETS hta EXPORT htaTargets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        INCLUDES DESTINATION include
    )
    install(EXPORT htaTargets
        FILE htaTargets.cmake
        NAMESPACE hta::
        DESTINATION lib/cmake/hta
    )

    include(CMakePackageConfigHelpers)
    write_basic_package_version_File("htaConfigVersion.cmake"
        VERSION ${hta_VERSION}
        COMPATIBILITY SameMajorVersion
    )
    install(FILES "htaConfig.cmake" "${CMAKE_CURRENT_BINARY_DIR}/htaConfigVersion.cmake"
        DESTINATION lib/cmake/hta
    )

    include(CTest)
    add_subdirectory(tests)
else()
    set_target_properties(hta PROPERTIES EXCLUDE_FROM_ALL TRUE)
endif()
