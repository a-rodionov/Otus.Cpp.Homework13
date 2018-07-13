#pragma once

#include <set>
#include <boost/asio.hpp>
#include <boost/log/trivial.hpp>
#include <boost/system/system_error.hpp>
#include "ThreadPool.h"
#include "DBCommands.h"
#include "coroutine.hpp"
#include "yield.hpp"

using boost::asio::ip::tcp;

const std::string SERVER_RESPONSE_OK{"OK"};
const std::string SERVER_RESPONSE_ERROR{"ERR"};

class join_server : public std::enable_shared_from_this<join_server>, boost::noncopyable
{

  class client_session;
  using session_set = std::shared_ptr<std::set<std::shared_ptr<client_session>>>;

  class client_session : public std::enable_shared_from_this<client_session>, public coroutine, boost::noncopyable
  {

    client_session(boost::asio::io_service& io_service,
                   session_set& sessions,
                   ThreadPool& threadPool)
    : io_service{io_service},
    socket(io_service),
    sessions(sessions),
    threadPool{threadPool} {};

  public:

    ~client_session() {
      stop();
    }

    static auto make(boost::asio::io_service& io_service,
                     session_set& sessions,
                     ThreadPool& threadPool) {
      return std::shared_ptr<client_session>(new client_session(io_service, sessions, threadPool));
    }

    tcp::socket& sock() {
      return socket;
    }

    void start() {
      if (isStarted)
        return;
      isStarted = true;
      step();
    }

    void stop() {
      if (!isStarted)
        return;
      isStarted = false;
      socket.close();
      auto itr = std::find(std::cbegin(*sessions), std::cend(*sessions), shared_from_this());
      sessions->erase(itr);
    }

  private:

    void step(const boost::system::error_code& ec = boost::system::error_code(), size_t bytes = 0) {
      reenter(this) {
        for (;;) {
          yield async_read_until( socket,
                                  read_buffer,
                                  '\n',
                                  [self = shared_from_this()] (boost::system::error_code ec, size_t) {
                                    if (ec) {
                                      self->stop();
                                      return;
                                    }
                                    self->step();
                                  });

          yield io_service.post( [self = shared_from_this()] () { self->on_client_cmd(); } );

          yield {
            if(ec) {
              async_write( socket,
                           write_buffer,
                           [self = shared_from_this()] (boost::system::error_code, size_t) {
                              self->stop();
                           });
            }
            else {
              async_write( socket,
                           write_buffer,
                           [self = shared_from_this()] (boost::system::error_code ec, size_t) {
                              if (ec) {
                                self->stop();
                                return;
                              }
                              self->step();
                           });
            }
          }
        }
      }
    }

    void on_client_cmd() {
      threadPool.AddTask(
        [self = shared_from_this()] () {
          try {
            std::istream istream(&self->read_buffer);
            std::string cmd;
            std::getline(istream, cmd);
            std::ostream ostream(&self->write_buffer);
            ExecuteDBCommad(cmd, ostream);
            ostream << SERVER_RESPONSE_OK << std::endl;
            self->step();
          }
          catch(DataBaseException& exc) {
            std::ostream ostream(&self->write_buffer);
            ostream << SERVER_RESPONSE_ERROR << ' ' << exc.what() << std::endl;
            self->step();
          }
          catch(std::exception& exc) {
            std::ostream ostream(&self->write_buffer);
            ostream << SERVER_RESPONSE_ERROR << ' ' << exc.what() << std::endl;
            self->step(make_error_code(boost::system::errc::errc_t::state_not_recoverable));
          }
        });
    }

    bool isStarted{false};
    boost::asio::io_service& io_service;
    tcp::socket socket;
    boost::asio::streambuf read_buffer;
    boost::asio::streambuf write_buffer;
    session_set sessions;
    ThreadPool& threadPool;
  };

  explicit join_server(unsigned short port_num)
    : io_service(),
      acceptor(io_service, tcp::endpoint{tcp::v4(), port_num}),
      sessions(std::make_shared<std::set<std::shared_ptr<client_session>>>())
  {
    for(size_t i{0}; i < std::thread::hardware_concurrency(); ++i) {
      threadPool.AddWorker();
    }
  }

public:

  ~join_server() {
    stop();
  }

  static auto make(unsigned short port_num) {
    return std::shared_ptr<join_server>(new join_server(port_num));
  }

  void start() {
    if(isStarted)
      return;
    isStarted = true;
    auto session = client_session::make(io_service, sessions, threadPool);
    do_accept(session);
    io_service.run();
  }

  void stop() {
    if(!isStarted)
      return;
    for(const auto& session : (*sessions)) {
      session->stop();
    }
    io_service.stop();
    isStarted = false;
  }

private:

  void do_accept(std::shared_ptr<client_session> session) {
    auto self = shared_from_this();
    acceptor.async_accept(session->sock(),
      [self, session](boost::system::error_code ec)
      {
        if (!ec) {
          self->sessions->insert(session);
          session->start();
        }
        auto new_session = client_session::make(self->io_service, self->sessions, self->threadPool);
        self->do_accept(new_session);
      });
  }

  bool isStarted{false};
  boost::asio::io_service io_service;
  tcp::acceptor acceptor;
  session_set sessions;     // сервер обрабатывает сессии в одном потоке, поэтому
                            // здесь не требуется блокировок. При этом он использует
                            // асинхронные операции ввода/вывода в сеть, что позволяет
                            // ему обрабатывать одновременно несколько соединений.
  ThreadPool threadPool;
};
