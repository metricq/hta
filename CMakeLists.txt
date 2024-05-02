cmake_minimum_required(VERSION 3.8)

project(hta)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")

include(cmake/DefaultBuildType.cmake)

find_package(StdFilesystem REQUIRED)


add_subdirectory(lib/catch)
add_subdirectory(lib/json)

set(hta_VERSION 0.0)

set(SRCS
    src/aggregate.cpp src/directory.cpp
    src/metric.cpp
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

if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/src/tools)

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
