#include <algorithm>
#include <limits>
#include "JoinServer.h"
#include "Logger.h"
#include <boost/asio.hpp>
#include <algorithm>

using boost::asio::ip::tcp;

int main(int argc, char const* argv[])
{
  try
  {
    unsigned long long port_num;
    try
    {
      if(2 != argc) {
        throw std::invalid_argument("");
      }

      std::string digit_str{argv[1]};
      if(!std::all_of(std::cbegin(digit_str),
                      std::cend(digit_str),
                      [](unsigned char symbol) { return std::isdigit(symbol); } )) {
        throw std::invalid_argument("");
      }
      port_num = std::stoull(digit_str);
      if(port_num > std::numeric_limits<unsigned short>::max()) {
        throw std::invalid_argument("");
      }
    }
    catch(...)
    {
      std::string error_msg = "The programm must be started with 1 parameter, which means the server port number. "
                              "The value must be in range 0 - "
                              + std::to_string(std::numeric_limits<unsigned short>::max());
      throw std::invalid_argument(error_msg);
    }

    Logger::Instance();

    auto backgroundThreadsCount = std::max(static_cast<unsigned int>(2), std::thread::hardware_concurrency());
    auto server = join_server::make(static_cast<unsigned short>(port_num), backgroundThreadsCount);
    server->start();
  }
  catch (const std::exception& e)
  {
    std::cerr << e.what() << std::endl;
  }
  return 0;
}
