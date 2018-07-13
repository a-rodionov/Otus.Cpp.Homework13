#include "ThreadPool.h"

#define BOOST_TEST_MODULE test_thread_pool

#include <boost/test/unit_test.hpp>
#include <boost/test/included/unit_test.hpp>

using namespace std::chrono_literals;


BOOST_AUTO_TEST_SUITE(test_suite_main)

BOOST_AUTO_TEST_CASE(adding_worker_threads)
{
  ThreadPool thread_pool;
  BOOST_REQUIRE_EQUAL(0, thread_pool.WorkersCount());

  thread_pool.AddWorker();
  thread_pool.AddWorker();
  thread_pool.AddWorker();
  BOOST_REQUIRE_EQUAL(3, thread_pool.WorkersCount());

  thread_pool.StopWorkers();
  BOOST_REQUIRE_EQUAL(0, thread_pool.WorkersCount());
}

BOOST_AUTO_TEST_CASE(verify_multithreaded_execution)
{
  ThreadPool thread_pool;
  std::thread::id first_thread_id{std::this_thread::get_id()};
  std::thread::id second_thread_id{std::this_thread::get_id()};
  BOOST_REQUIRE_EQUAL(first_thread_id, second_thread_id);

  thread_pool.AddWorker();
  thread_pool.AddWorker();
  thread_pool.AddTask([&first_thread_id] () {
    first_thread_id = std::this_thread::get_id();
    std::this_thread::sleep_for(200ms);
  });
  thread_pool.AddTask([&second_thread_id] () {
    second_thread_id = std::this_thread::get_id();
    std::this_thread::sleep_for(200ms);
  });
  thread_pool.StopWorkers();

  BOOST_REQUIRE(first_thread_id != second_thread_id);
  BOOST_REQUIRE(std::this_thread::get_id() != first_thread_id);
  BOOST_REQUIRE(std::this_thread::get_id() != second_thread_id);
}

BOOST_AUTO_TEST_CASE(adding_tasks_before_start)
{
  ThreadPool thread_pool;
  std::string result;

  thread_pool.AddTask([&result](){ result += "1st part."; });

  thread_pool.AddWorker();  
  thread_pool.AddTask([&result](){ result += "2nd part."; });
  thread_pool.StopWorkers();

  BOOST_REQUIRE_EQUAL("1st part.2nd part.", result);
}

BOOST_AUTO_TEST_CASE(adding_tasks_after_stop)
{
  ThreadPool thread_pool;
  std::string result;

  thread_pool.AddWorker();
  thread_pool.AddTask([&result](){ result += "1st part."; });
  thread_pool.AddTask([&result](){ result += "2nd part."; });

  thread_pool.StopWorkers();

  thread_pool.AddTask([&result](){ result += "Data won't be processed."; });
  std::this_thread::sleep_for(200ms);

  BOOST_REQUIRE_EQUAL("1st part.2nd part.", result);
}

BOOST_AUTO_TEST_CASE(restarting)
{
  ThreadPool thread_pool;
  std::string result;

  thread_pool.AddWorker();
  thread_pool.AddTask([&result](){ result += "1st part."; });
  thread_pool.AddTask([&result](){ result += "2nd part."; });
  thread_pool.StopWorkers();
  BOOST_REQUIRE_EQUAL(0, thread_pool.WorkersCount());

  thread_pool.AddWorker();
  thread_pool.AddTask([&result](){ result += "Data processed after adding workers."; });
  thread_pool.StopWorkers();

  BOOST_REQUIRE_EQUAL("1st part.2nd part.Data processed after adding workers.", result);
}

BOOST_AUTO_TEST_CASE(handle_exception_from_thread)
{
  ThreadPool thread_pool;
  thread_pool.AddWorker();
  thread_pool.AddTask([](){ throw std::runtime_error("Exception from thread pool."); });
  thread_pool.StopWorkers();

  auto exc = thread_pool.GetLastException();
  BOOST_REQUIRE(nullptr != exc);
  BOOST_CHECK_THROW(std::rethrow_exception(exc), std::runtime_error);
}


BOOST_AUTO_TEST_SUITE_END()
