project(hta)

cmake_minimum_required(VERSION 3.8)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake;${CMAKE_MODULE_PATH}")

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
        if(${CMAKE_CXX_COMPILER_ID} STREQUAL GNU)
            # gcc needs an extra lib for filesystem
            set_target_properties(std::filesystem PROPERTIES INTERFACE_LINK_LIBRARIES stdc++fs)
        endif()
    endif()
else()
    set_target_properties(std::filesystem PROPERTIES INTERFACE_COMPILE_DEFINITIONS HAS_STD_FILESYSTEM)
endif()

add_subdirectory(lib/catch)
add_subdirectory(lib/json)

set(hta_VERSION 0.0)

set(SRCS
    src/aggregate.cpp src/directory.cpp src/metric.cpp
    src/storage/file/directory.cpp src/storage/file/metric.cpp
)

add_library(hta ${SRCS})
target_include_directories(hta
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(hta
    PRIVATE
        json::json
        std::filesystem
)

target_compile_features(hta PUBLIC cxx_std_17)

add_library(hta::hta ALIAS hta)

if(CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    target_compile_options(hta
        INTERFACE
            -Wall -Wextra -pedantic
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
