add_library(catch INTERFACE)
target_include_directories(catch INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)
add_library(catch::catch ALIAS catch)
