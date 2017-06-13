# Part of Beast

GroupSources(examples/ examples)
GroupSources(extras/beast extras)
GroupSources(include/beast beast)
GroupSources(test/examples "/")

add_executable (examples
    ${BEAST_INCLUDES}
    ${EXAMPLES_INCLUDES}
    ${EXTRAS_INCLUDES}
    main.cpp
    server.cpp
)

if (NOT WIN32)
    target_link_libraries(examples ${Boost_LIBRARIES} Threads::Threads)
else()
    target_link_libraries(examples ${Boost_LIBRARIES})
endif()
