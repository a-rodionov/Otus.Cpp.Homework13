#pragma once

#include <memory>
#include <list>
#include <boost/asio.hpp>

class DBResponse
{
public:
  virtual ~DBResponse(){};

  virtual void push_back(const std::string&) = 0;

  virtual void push_back(std::string&&) = 0;
};

class DBResponseNoCache : public DBResponse, public std::enable_shared_from_this<DBResponseNoCache>
{
  DBResponseNoCache(std::shared_ptr<boost::asio::ip::tcp::socket>& socket)
    : socket{socket} {}

  void send(const std::string& response) {
    auto itr = sendingData.insert(sendingData.cend(), std::move(response));
    async_write( *socket,
                 boost::asio::buffer(*itr),
                 [self = shared_from_this(), itr] (boost::system::error_code, size_t) {
                  self->sendingData.erase(itr);
                 });
  }

public:

  ~DBResponseNoCache(){}

  static auto make(std::shared_ptr<boost::asio::ip::tcp::socket>& socket) {
    return std::shared_ptr<DBResponseNoCache>(new DBResponseNoCache(socket));
  }

  void push_back(const std::string& response) override {
    std::string tmp{response};
    tmp += '\n';
    send(tmp);
  }

  void push_back(std::string&& response) override {
    response += '\n';
    send(response);
  }

private:
  std::shared_ptr<boost::asio::ip::tcp::socket> socket;
  std::list<std::string> sendingData;
};
