#pragma once

#include <memory>
#include <iostream>
#include "DataBase.h"

static const std::string CMD_INSERT{"INSERT"};
static const std::string CMD_TRUNCATE{"TRUNCATE"};
static const std::string CMD_INTERSECTION{"INTERSECTION"};
static const std::string CMD_SYMMETRIC_DIFFERENCE{"SYMMETRIC_DIFFERENCE"};
static const std::string CMD_PAUSED_IN_SYMMETRIC_DIFFERENCE{"PAUSED_IN_SYMMETRIC_DIFFERENCE"};  // для тестов

const std::string ERR_EMPTY_CMD{"Command wasn't provided."};
const std::string ERR_UNSUPPORTED_CMD{"Database command is not supported."};
const std::string ERR_PARAMS_COUNT{"Wrong number of parameters was provided."};
const std::string ERR_PARSE_DIGIT{"Failed to parse digit."};

static auto split(const std::string& str_command) {
  std::vector<std::string> parts;
  if(str_command.empty()) {
    return parts;
  }

  size_t prev = 0, pos = 0;
  do
  {
    pos = str_command.find(' ', prev);
    parts.push_back(str_command.substr(prev, pos-prev));
    prev = pos + 1;
  }
  while (pos < str_command.length() && prev < str_command.length());
  return parts;
}

static void ExecuteInsert(const std::vector<std::string>& cmd) {
  if(4 != cmd.size()) {
    throw DataBaseException(ERR_PARAMS_COUNT);
  }

  unsigned long long index;
  try {
    if(!std::all_of(std::cbegin(cmd[2]),
                    std::cend(cmd[2]),
                    [](unsigned char symbol) { return std::isdigit(symbol); } )) {
      throw std::invalid_argument("");
    }
    index = std::stoull(cmd[2]);
  }
  catch(...) {
    throw DataBaseException(ERR_PARSE_DIGIT);
  }

  DataBase::Instance().GetTable(cmd[1])->Insert(std::make_pair(index, cmd[3]));
}

static void ExecuteTruncate(const std::vector<std::string>& cmd) {
  if(2 != cmd.size()) {
    throw DataBaseException(ERR_PARAMS_COUNT);
  }
  DataBase::Instance().TruncateTable(cmd[1]);
}

static void ExecuteIntersection(const std::vector<std::string>& cmd, std::ostream& out) {
  if(1 != cmd.size()) {
    throw DataBaseException(ERR_PARAMS_COUNT);
  }
  auto table_a = DataBase::Instance().GetTable("A");
  auto table_b = DataBase::Instance().GetTable("B");
  auto result = table_a->Intersection(*table_b);
  std::copy(std::cbegin(*result), std::cend(*result),
            std::ostream_iterator<set_operation_result>(out, "\n"));
}

static void ExecuteSymmetricDifference(const std::vector<std::string>& cmd, std::ostream& out) {
  if(1 != cmd.size()) {
    throw DataBaseException(ERR_PARAMS_COUNT);
  }
  auto table_a = DataBase::Instance().GetTable("A");
  auto table_b = DataBase::Instance().GetTable("B");
  auto result = table_a->SymmetricDifference(*table_b);
  std::copy(std::cbegin(*result), std::cend(*result),
            std::ostream_iterator<set_operation_result>(out, "\n"));
}

static void ExecutePausedInSymmetricDifference(const std::vector<std::string>& cmd, std::ostream& out) {
  if(2 != cmd.size()) {
    throw DataBaseException(ERR_PARAMS_COUNT);
  }

  unsigned long long seconds;
  try {
    if(!std::all_of(std::cbegin(cmd[1]),
                    std::cend(cmd[1]),
                    [](unsigned char symbol) { return std::isdigit(symbol); } )) {
      throw std::invalid_argument("");
    }
    seconds = std::stoull(cmd[1]);
  }
  catch(...) {
    throw DataBaseException(ERR_PARSE_DIGIT);
  }

  auto table_a = DataBase::Instance().GetTable("A");
  auto table_b = DataBase::Instance().GetTable("B");
  auto result = table_a->PauseInSymmetricDifference(*table_b, seconds);
  std::copy(std::cbegin(*result), std::cend(*result),
            std::ostream_iterator<set_operation_result>(out, "\n"));
}

void ExecuteDBCommad(const std::string& str_command, std::ostream& out) {
  auto cmd = split(str_command);
  if(cmd.empty()) {
    throw DataBaseException(ERR_EMPTY_CMD);
  }

  if(CMD_INSERT == cmd[0]) {
    ExecuteInsert(cmd);
  }
  else if(CMD_TRUNCATE == cmd[0]) {
    ExecuteTruncate(cmd);
  }
  else if(CMD_INTERSECTION == cmd[0]) {
    ExecuteIntersection(cmd, out);
  }
  else if(CMD_SYMMETRIC_DIFFERENCE == cmd[0]) {
    ExecuteSymmetricDifference(cmd, out);
  }
  else if(CMD_PAUSED_IN_SYMMETRIC_DIFFERENCE == cmd[0]) {
    ExecutePausedInSymmetricDifference(cmd, out);
  }
  else
    throw DataBaseException(ERR_UNSUPPORTED_CMD);
}
