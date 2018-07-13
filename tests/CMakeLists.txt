cmake_minimum_required(VERSION 3.2)

function(test name)
  add_executable (${name} ${name}.cpp)
  target_compile_definitions(${name} PRIVATE BOOST_TEST_DYN_LINK BOOST_LOG_DYN_LINK)
  target_include_directories(${name} PRIVATE ${PROJECT_SOURCE_DIR})
  target_link_libraries (${name} 
      ${Boost_LIBRARIES}
      ${CMAKE_THREAD_LIBS_INIT}
  )
  add_test(${name} ${CMAKE_BINARY_DIR}/bin/${name})
endfunction(test)

test(test_thread_pool)
test(test_boost_log)
test(test_server)
