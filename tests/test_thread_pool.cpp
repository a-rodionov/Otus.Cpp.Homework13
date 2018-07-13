#include "ThreadPool.h"

#define BOOST_TEST_MODULE test_thread_pool

#include <boost/test/unit_test.hpp>
#include <boost/test/included/unit_test.hpp>

using namespace std::chrono_literals;


BOOST_AUTO_TEST_SUITE(test_suite_main)

BOOST_AUTO_TEST_CASE(adding_worker_threads)
{
  BOOST_REQUIRE_EQUAL(0, ThreadPool::Instance().WorkersCount());

  ThreadPool::Instance().AddWorker();
  ThreadPool::Instance().AddWorker();
  ThreadPool::Instance().AddWorker();
  BOOST_REQUIRE_EQUAL(3, ThreadPool::Instance().WorkersCount());

  ThreadPool::Instance().StopWorkers();
  BOOST_REQUIRE_EQUAL(0, ThreadPool::Instance().WorkersCount());
}

BOOST_AUTO_TEST_CASE(verify_multithreaded_execution)
{
  std::thread::id first_thread_id{std::this_thread::get_id()};
  std::thread::id second_thread_id{std::this_thread::get_id()};
  BOOST_REQUIRE_EQUAL(first_thread_id, second_thread_id);

  ThreadPool::Instance().AddWorker();
  ThreadPool::Instance().AddWorker();
  ThreadPool::Instance().AddTask([&first_thread_id] () {
    first_thread_id = std::this_thread::get_id();
    std::this_thread::sleep_for(200ms);
  });
  ThreadPool::Instance().AddTask([&second_thread_id] () {
    second_thread_id = std::this_thread::get_id();
    std::this_thread::sleep_for(200ms);
  });
  ThreadPool::Instance().StopWorkers();

  BOOST_REQUIRE(first_thread_id != second_thread_id);
  BOOST_REQUIRE(std::this_thread::get_id() != first_thread_id);
  BOOST_REQUIRE(std::this_thread::get_id() != second_thread_id);
}

BOOST_AUTO_TEST_CASE(adding_tasks_before_start)
{
  std::string result;

  ThreadPool::Instance().AddTask([&result](){ result += "1st part."; });

  ThreadPool::Instance().AddWorker();  
  ThreadPool::Instance().AddTask([&result](){ result += "2nd part."; });
  ThreadPool::Instance().StopWorkers();

  BOOST_REQUIRE_EQUAL("1st part.2nd part.", result);
}

BOOST_AUTO_TEST_CASE(adding_tasks_after_stop)
{
  std::string result;

  ThreadPool::Instance().AddWorker();
  ThreadPool::Instance().AddTask([&result](){ result += "1st part."; });
  ThreadPool::Instance().AddTask([&result](){ result += "2nd part."; });

  ThreadPool::Instance().StopWorkers();

  ThreadPool::Instance().AddTask([&result](){ result += "Data won't be processed."; });
  std::this_thread::sleep_for(200ms);

  BOOST_REQUIRE_EQUAL("1st part.2nd part.", result);
}

BOOST_AUTO_TEST_CASE(restarting)
{
  std::string result;

  ThreadPool::Instance().AddWorker();
  ThreadPool::Instance().AddTask([&result](){ result += "1st part."; });
  ThreadPool::Instance().AddTask([&result](){ result += "2nd part."; });
  ThreadPool::Instance().StopWorkers();
  BOOST_REQUIRE_EQUAL(0, ThreadPool::Instance().WorkersCount());

  ThreadPool::Instance().AddWorker();
  ThreadPool::Instance().AddTask([&result](){ result += "Data processed after adding workers."; });
  ThreadPool::Instance().StopWorkers();

  BOOST_REQUIRE_EQUAL("1st part.2nd part.Data processed after adding workers.", result);
}
/*
BOOST_AUTO_TEST_CASE(handle_exception_from_thread)
{
  ThreadPool::Instance().AddWorker();
  ThreadPool::Instance().AddTask([](){ throw std::runtime_error("Exception from thread pool."); });
  ThreadPool::Instance().StopWorkers();

  auto exc = ThreadPool::Instance().GetLastException();
  BOOST_REQUIRE(nullptr != exc);
  BOOST_CHECK_THROW(std::rethrow_exception(exc), std::runtime_error);
}
*/

BOOST_AUTO_TEST_SUITE_END()
