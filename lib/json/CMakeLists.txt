if(NOT TARGET json::json)
    add_library(json INTERFACE)
    target_include_directories(json SYSTEM INTERFACE
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:include>
    )
    add_library(json::json ALIAS json)

    install(TARGETS json EXPORT jsonTargets
            LIBRARY DESTINATION lib
            ARCHIVE DESTINATION lib
            RUNTIME DESTINATION bin
            INCLUDES DESTINATION include
    )

    install(EXPORT jsonTargets
            FILE jsonTargets.cmake
            NAMESPACE json::
            DESTINATION lib/cmake/json
    )
endif()
