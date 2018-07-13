#pragma once

#include <chrono>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/utility/setup/file.hpp>

class Logger
{
  
public:

  static Logger& Instance()
  {
    static Logger logger;
    return logger;
  }

  std::string GetFilename() const {
    return filename;
  }

  void Flush() const {
    pLogSink->flush();
  }

private:

  Logger() {
    std::stringstream filename_ctr;
    filename_ctr << "bulkmt_error_" 
      << std::to_string(
            std::chrono::duration_cast<std::chrono::seconds>(
              std::chrono::system_clock::now().time_since_epoch()
            ).count())
      << ".log";
    filename = filename_ctr.str();
    pLogSink = boost::log::add_file_log(filename);
    boost::log::core::get()->set_filter (
      boost::log::trivial::severity >= boost::log::trivial::info
    );

  }
  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;
  Logger(const Logger&&) = delete;
  Logger& operator=(const Logger&&) = delete;

  std::string filename;
  decltype(boost::log::add_file_log("")) pLogSink;
};
