#include <fstream>
#include <thread>
#include <boost/log/trivial.hpp>
#include "Logger.h"

#define BOOST_TEST_MODULE test_boost_log

#include <boost/test/unit_test.hpp>
#include <boost/test/included/unit_test.hpp>


BOOST_AUTO_TEST_SUITE(test_suite_main)

BOOST_AUTO_TEST_CASE(flush_incomplete_block_by_end)
{
  std::string logContent;
  std::string errorMsg{"Some error text from test."};

  Logger::Instance();
  BOOST_LOG_TRIVIAL(error) << errorMsg;
  Logger::Instance().Flush();

  std::ifstream ifs{Logger::Instance().GetFilename().c_str(), std::ifstream::in};
  BOOST_REQUIRE_EQUAL(false, ifs.fail());

  std::getline(ifs, logContent);
  BOOST_REQUIRE( std::string::npos != logContent.find(errorMsg) );

  ifs.close();

  std::system("rm -f *.log");
}

BOOST_AUTO_TEST_SUITE_END()
