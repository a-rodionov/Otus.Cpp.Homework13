#include <algorithm>
#include <thread>
#include <chrono>
#include <boost/asio.hpp>
#include <boost/filesystem.hpp>
#include <boost/regex.hpp>

#define BOOST_TEST_IGNORE_NON_ZERO_CHILD_CODE
#define BOOST_TEST_MODULE test_async

#include <boost/test/unit_test.hpp>
#include <boost/test/included/unit_test.hpp>

#include "JoinServer.h"

using namespace boost::asio;
using boost::asio::ip::tcp;
using namespace std::chrono_literals;


std::string GetCurrentWorkingDir( void ) {
  char buff[FILENAME_MAX];
  ssize_t len = sizeof(buff);
  int bytes = std::min(readlink("/proc/self/exe", buff, len), len - 1);
  if(bytes >= 0)
      buff[bytes] = '\0';
  std::string current_working_dir(buff);
  return current_working_dir.substr(0, current_working_dir.find_last_of('/')+1);
}

void StartServer(const std::string& cmd, std::atomic_bool& isThreadStarted) {
  try {
    isThreadStarted = true;
    std::system(cmd.c_str());
  }
  catch(...) {}
}

void Transmit(ip::tcp::socket& socket,
              boost::asio::streambuf& streambuf,
              const std::string& client_request,
              std::vector<std::string>& server_response) {
  server_response.clear();
  socket.write_some(buffer(client_request));
  std::string line;
  while(true) {
    boost::asio::read_until(socket, streambuf, '\n');
    std::istream istream(&streambuf);
    while(std::getline(istream, line)) {
      server_response.push_back(line);
    }
    if(SERVER_RESPONSE_OK == server_response.back()
      || 0 == server_response.back().find(SERVER_RESPONSE_ERROR))
      break;
  }
}

struct initialized_server
{
  initialized_server()
    : io_service(),
    socket(io_service)
  {
    std::atomic_bool isThreadStarted{false};
    auto cmd = GetCurrentWorkingDir();
    cmd += "join_server 9000";
    server_thread = std::make_unique<std::thread>(StartServer, cmd, std::ref(isThreadStarted));
    while(!isThreadStarted) {}
    std::this_thread::sleep_for(200ms);

    ip::tcp::endpoint ep( ip::address::from_string("127.0.0.1"), 9000);
    socket.connect(ep);
  }

  ~initialized_server() {
    std::system("killall join_server");
    server_thread->join();
  }

  std::unique_ptr<std::thread> server_thread;
  boost::asio::io_service io_service;
  ip::tcp::socket socket;
  boost::asio::streambuf read_buffer;
  std::vector<std::string> server_response;
};

BOOST_FIXTURE_TEST_SUITE(test_suite_main, initialized_server)

BOOST_AUTO_TEST_CASE(insert_duplicates)
{
  Transmit(socket, read_buffer, "INSERT A 0 lean\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 1 lean\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT B 0 lean\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 0 sweater\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL("ERR duplicate 0", server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 1 sweater\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL("ERR duplicate 1", server_response[0]);
}

BOOST_AUTO_TEST_CASE(intersection)
{
  // Вставка в разном порядке следования индекса

  Transmit(socket, read_buffer, "INSERT A 0 lean\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 1 sweater\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 3 violation\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 2 frank\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT B 2 proposal\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT B 3 example\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT B 5 flour\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT B 4 lake\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INTERSECTION\n", server_response);
  BOOST_REQUIRE_EQUAL(3, server_response.size());
  BOOST_REQUIRE_EQUAL("2,frank,proposal", server_response[0]);
  BOOST_REQUIRE_EQUAL("3,violation,example", server_response[1]);
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[2]);
}

BOOST_AUTO_TEST_CASE(symmetric_difference)
{
  // Вставка в разном порядке следования индекса

  Transmit(socket, read_buffer, "INSERT A 0 lean\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 1 sweater\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 3 violation\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 2 frank\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT B 2 proposal\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT B 3 example\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT B 5 flour\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT B 4 lake\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "SYMMETRIC_DIFFERENCE\n", server_response);
  BOOST_REQUIRE_EQUAL(5, server_response.size());
  BOOST_REQUIRE_EQUAL("0,lean,", server_response[0]);
  BOOST_REQUIRE_EQUAL("1,sweater,", server_response[1]);
  BOOST_REQUIRE_EQUAL("4,,lake", server_response[2]);
  BOOST_REQUIRE_EQUAL("5,,flour", server_response[3]);
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[4]);
}

BOOST_AUTO_TEST_CASE(truncate)
{
  Transmit(socket, read_buffer, "INSERT A 0 lean\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 1 sweater\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 2 frank\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 3 violation\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT B 2 proposal\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT B 3 example\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT B 4 lake\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT B 5 flour\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "TRUNCATE A\n", server_response);

  Transmit(socket, read_buffer, "INTERSECTION\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "SYMMETRIC_DIFFERENCE\n", server_response);
  BOOST_REQUIRE_EQUAL(5, server_response.size());
  BOOST_REQUIRE_EQUAL("2,,proposal", server_response[0]);
  BOOST_REQUIRE_EQUAL("3,,example", server_response[1]);
  BOOST_REQUIRE_EQUAL("4,,lake", server_response[2]);
  BOOST_REQUIRE_EQUAL("5,,flour", server_response[3]);
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[4]);
}

void ParallelDBRead(std::atomic_bool& isDataSent,
                    std::atomic_bool& isDataRead,
                    std::atomic_bool& isFailed,
                    const std::string& client_request,
                    std::vector<std::string>& server_response) {
  try {
    boost::asio::io_service io_service;
    ip::tcp::endpoint ep( ip::address::from_string("127.0.0.1"), 9000);
    ip::tcp::socket socket(io_service);
    socket.connect(ep);

    socket.write_some(buffer(client_request));
    isDataSent = true;

    server_response.clear();
    std::string line;
    while(true) {
      boost::asio::streambuf streambuf;
      boost::asio::read_until(socket, streambuf, '\n');
      std::istream istream(&streambuf);
      while(std::getline(istream, line)) {
        server_response.push_back(line);
      }
      if(SERVER_RESPONSE_OK == server_response.back()
        || 0 == server_response.back().find(SERVER_RESPONSE_ERROR))
        break;
    }
    isDataRead = true;
  }
  catch(...) {
    isFailed = true;
  }
}

// Тест сброса таблицы в процессе ее чтения
BOOST_AUTO_TEST_CASE(parallel_truncate)
{
  Transmit(socket, read_buffer, "INSERT A 0 lean\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 1 sweater\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  std::atomic_bool isDataSent{false};
  std::atomic_bool isDataRead{false};
  std::atomic_bool isFailed{false};
  std::vector<std::string> parallel_paused_in_symmetric_difference;
  std::thread parallel_read(ParallelDBRead,
                            std::ref(isDataSent),
                            std::ref(isDataRead),
                            std::ref(isFailed),
                            "PAUSED_IN_SYMMETRIC_DIFFERENCE 5\n",
                            std::ref(parallel_paused_in_symmetric_difference));

  while(!isDataSent) {} // spin lock
  std::this_thread::sleep_for(200ms);

  Transmit(socket, read_buffer, "TRUNCATE A\n", server_response);

  std::vector<std::string> local_symmetric_difference;
  Transmit(socket, read_buffer, "SYMMETRIC_DIFFERENCE\n", local_symmetric_difference);

  BOOST_CHECK_EQUAL(false, isDataRead); // удостовериться, что в другом потоке данные еще не прочитаны
  parallel_read.join();                 // дожидаемся завершения чтения в другом потоке
  BOOST_CHECK_EQUAL(false, isFailed);

  // Проверка успешного завершения операции сброса таблицыво время блокировки на чтение другим потоком
  BOOST_CHECK_EQUAL(1, server_response.size());
  BOOST_CHECK_EQUAL(SERVER_RESPONSE_OK, server_response.at(0));

  // Проверка результата выполнения сброса таблицы
  BOOST_CHECK_EQUAL(1, local_symmetric_difference.size());
  BOOST_CHECK_EQUAL(SERVER_RESPONSE_OK, local_symmetric_difference.at(0));

  // Проверка того, что сброс талблицы не повлиял на выполняющуюся в данный момент операцию чтения
  BOOST_CHECK_EQUAL(3, parallel_paused_in_symmetric_difference.size());
  BOOST_CHECK_EQUAL("0,lean,", parallel_paused_in_symmetric_difference.at(0));
  BOOST_CHECK_EQUAL("1,sweater,", parallel_paused_in_symmetric_difference.at(1));
  BOOST_CHECK_EQUAL(SERVER_RESPONSE_OK, parallel_paused_in_symmetric_difference.at(2));
}

// Тест вставки в таблицу в процессе ее чтения
BOOST_AUTO_TEST_CASE(parallel_insert)
{
  Transmit(socket, read_buffer, "INSERT A 0 lean\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 1 sweater\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  Transmit(socket, read_buffer, "INSERT B 3 example\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  std::atomic_bool isDataSent{false};
  std::atomic_bool isDataRead{false};
  std::atomic_bool isFailed{false};
  std::vector<std::string> parallel_paused_in_symmetric_difference;
  std::thread parallel_read(ParallelDBRead,
                            std::ref(isDataSent),
                            std::ref(isDataRead),
                            std::ref(isFailed),
                            "PAUSED_IN_SYMMETRIC_DIFFERENCE 5\n",
                            std::ref(parallel_paused_in_symmetric_difference));

  while(!isDataSent) {} // spin lock
  std::this_thread::sleep_for(200ms);

  // Попытка вставки уже существующего идетификатора во время блокировки на чтение
  Transmit(socket, read_buffer, "INSERT A 0 lean\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL("ERR duplicate 0", server_response[0]);

  // Попытка вставки нового идетификатора во время блокировки на чтение
  Transmit(socket, read_buffer, "INSERT A 2 frank\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_OK, server_response[0]);

  // Попытка вставки уже существующего идетификатора, добавленного во время блокировки на чтение
  Transmit(socket, read_buffer, "INSERT A 2 lean\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL("ERR duplicate 2", server_response[0]);

  BOOST_CHECK_EQUAL(false, isDataRead); // удостовериться, что в другом потоке данные еще не прочитаны
  parallel_read.join();                 // дожидаемся завершения чтения в другом потоке
  BOOST_CHECK_EQUAL(false, isFailed);

  // Операция чтения, которая была инициирована до серии добавления новых данных, новых данных не получила
  BOOST_CHECK_EQUAL(4, parallel_paused_in_symmetric_difference.size());
  BOOST_CHECK_EQUAL("0,lean,", parallel_paused_in_symmetric_difference.at(0));
  BOOST_CHECK_EQUAL("1,sweater,", parallel_paused_in_symmetric_difference.at(1));
  BOOST_CHECK_EQUAL("3,,example", parallel_paused_in_symmetric_difference.at(2));
  BOOST_CHECK_EQUAL(SERVER_RESPONSE_OK, parallel_paused_in_symmetric_difference.at(3));

  // Операция чтения, которая была инициирована после серии добавления новых данных, новые данные получила
  std::vector<std::string> local_symmetric_difference;
  Transmit(socket, read_buffer, "SYMMETRIC_DIFFERENCE\n", local_symmetric_difference);
  BOOST_CHECK_EQUAL(5, local_symmetric_difference.size());
  BOOST_CHECK_EQUAL("0,lean,", local_symmetric_difference.at(0));
  BOOST_CHECK_EQUAL("1,sweater,", local_symmetric_difference.at(1));
  BOOST_CHECK_EQUAL("2,frank,", local_symmetric_difference.at(2));
  BOOST_CHECK_EQUAL("3,,example", local_symmetric_difference.at(3));
  BOOST_CHECK_EQUAL(SERVER_RESPONSE_OK, local_symmetric_difference.at(4));
}

BOOST_AUTO_TEST_CASE(insert_wrong_table)
{
  Transmit(socket, read_buffer, "INSERT C 0 lean\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_ERROR + ' ' + ERR_TABLE_NOT_FOUND, server_response[0]);
}

BOOST_AUTO_TEST_CASE(insert_wrong_number_of_params)
{
  Transmit(socket, read_buffer, "INSERT\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_ERROR + ' ' + ERR_PARAMS_COUNT, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_ERROR + ' ' + ERR_PARAMS_COUNT, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 0\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_ERROR + ' ' + ERR_PARAMS_COUNT, server_response[0]);

  Transmit(socket, read_buffer, "INSERT A 0 lean extra_param\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_ERROR + ' ' + ERR_PARAMS_COUNT, server_response[0]);
}

BOOST_AUTO_TEST_CASE(truncate_wrong_table)
{
  Transmit(socket, read_buffer, "TRUNCATE C\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_ERROR + ' ' + ERR_TABLE_NOT_FOUND, server_response[0]);
}

BOOST_AUTO_TEST_CASE(truncate_wrong_number_of_params)
{
  Transmit(socket, read_buffer, "TRUNCATE\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_ERROR + ' ' + ERR_PARAMS_COUNT, server_response[0]);

  Transmit(socket, read_buffer, "TRUNCATE A extra_param\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_ERROR + ' ' + ERR_PARAMS_COUNT, server_response[0]);
}

BOOST_AUTO_TEST_CASE(intersection_wrong_number_of_params)
{
  Transmit(socket, read_buffer, "INTERSECTION extra_param\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_ERROR + ' ' + ERR_PARAMS_COUNT, server_response[0]);
}

BOOST_AUTO_TEST_CASE(symmetric_difference_wrong_number_of_params)
{
  Transmit(socket, read_buffer, "SYMMETRIC_DIFFERENCE extra_param\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_ERROR + ' ' + ERR_PARAMS_COUNT, server_response[0]);
}

BOOST_AUTO_TEST_CASE(unsupported_command)
{
  Transmit(socket, read_buffer, "SHUFFLE\n", server_response);
  BOOST_REQUIRE_EQUAL(1, server_response.size());
  BOOST_REQUIRE_EQUAL(SERVER_RESPONSE_ERROR + ' ' + ERR_UNSUPPORTED_CMD, server_response[0]);
}

BOOST_AUTO_TEST_SUITE_END()
