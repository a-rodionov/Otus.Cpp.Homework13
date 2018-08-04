#pragma once

#include <iostream>
#include <map>
#include <list>
#include <atomic>
#include <mutex>
#include <shared_mutex>
#include "DBResponse.h"

const std::string ERR_TABLE_NOT_FOUND{"Table wasn't found."};
const std::string ERR_DUPLICATE{"duplicate"};

class DataBaseException : public std::runtime_error
{
public:
  DataBaseException(const std::string& errMsg)
    : std::runtime_error(errMsg) {}
};
/*
struct set_operation_result {

  set_operation_result(size_t idx, const std::string& value_1, const std::string& value_2)
    : idx{idx}, value_1{value_1}, value_2{value_2} {}

  operator std::string() {
    return std::string(std::to_string(idx) + ',' + value_1 + ',' + value_2);
  }

  size_t idx;
  std::string value_1;
  std::string value_2;
};

std::ostream& operator<<(std::ostream& out, const set_operation_result& data) {
  out << data.idx << ',' << data.value_1 << ',' << data.value_2;
  return out;
}
*/
std::string DBResponseToString(size_t idx, const std::string& value_1, const std::string& value_2) {
  return std::string(std::to_string(idx) + ',' + value_1 + ',' + value_2);
}

using row = std::pair<size_t, std::string>;

class DataTable {

  void InsertToCollection(std::map<size_t, std::string>& collection, const row& new_data) {
    auto result = collection.insert(new_data);
    if(result.second) {
      return;
    }
    std::string errMsg{ERR_DUPLICATE};
    errMsg += ' ' + std::to_string(new_data.first);
    throw DataBaseException(errMsg);
  }

  // Функция сохранения данных в основную коллекцию с проверкой уникальности индекса.
  bool TryInsert(const row& new_data) {
    std::unique_lock<std::shared_timed_mutex> lock_write_data(data_mutex, std::try_to_lock);
    // Не удалось захватить мьюетекс на запись
    if(!lock_write_data) {
      return false;
    }
    InsertToCollection(data, new_data);
    return true;
  }

  // Функция сохранения данных в коллекцию с отложенными для вставки данными с проверкой уникальности индекса.
  void DefferData(const row& new_data) {
    // Захватим мьютекс, защищающий основные данные, для чтения.
    // Также захватим мьютекс, защищающий еще не добавленные данные, для записи.
    // Важно соблюдать порядок блокировок для избежания взаимоблокировок.
    std::shared_lock<std::shared_timed_mutex> lock_read_data(data_mutex);
    std::lock_guard<std::mutex> lock_write_deffered__data(deferred_data_mutex);

    // Проверяем наличие в основной коллекции и кэше. Если такого индекса еще нет,
    // то сохраняем новые данные в кэше.
    if(data.count(new_data.first)) {
      std::string errMsg{ERR_DUPLICATE};
      errMsg += ' ' + std::to_string(new_data.first);
      throw DataBaseException(errMsg);
    }
    InsertToCollection(deferred_data, new_data);
  }

  // Функция перемещения отложенных для вставки данных в основную коллекцию.
  bool TryMoveDefferedData() {
    std::unique_lock<std::shared_timed_mutex> lock_write_data(data_mutex, std::try_to_lock);
    if(!lock_write_data) {
      // Кто-то продолжает читать.
      return false;
    }
    std::lock_guard<std::mutex> lock_write_deffered__data(deferred_data_mutex);
    if(!deferred_data.empty()) {
      std::copy(std::cbegin(deferred_data), std::cend(deferred_data), std::inserter(data, std::end(data)));
    }
    lock_write_data.unlock();
    deferred_data.clear();
    return true;
  }

public:

  void Insert(const row& new_data) {
    if(TryInsert(new_data))
      return;
    DefferData(new_data);
    // Возможна ситуация, когда последний "читающий" закончит работу во время выполнения
    // DefferData в этом потоке. Это возможно так как DefferData захватывает data_mutex
    // для чтения, а TryMoveDefferedData в конце работы "читающего" лишь пытается захватить
    // data_mutex для записи и выходит при неудаче. Данные отсанутся недобавленными, поэтому
    // необходимо попытаться их добавить самостоятельно.
    TryMoveDefferedData();
  }

  // Не был использован алгоритм из stl, чтобы избежать двойных проходов
  // по коллекциям таблиц из-за невозможности конструирования объекта
  // результата set_operation_result, содержащего конечный результат только
  // лишь из одного итератора на std::map<size_t, std::string>.
  void Intersection(DataTable& other, std::shared_ptr<DBResponse>& dbResponse) {

    // Оформлено в виде блока, чтобы не писать unlock
    {
      std::shared_lock<std::shared_timed_mutex> lock_read_data(data_mutex, std::defer_lock);
      std::shared_lock<std::shared_timed_mutex> lock_read_other_data(data_mutex, std::defer_lock);
      std::lock(lock_read_data, lock_read_other_data);

      auto first1 = std::cbegin(data);
      auto last1 = std::cend(data);
      auto first2 = std::cbegin(other.data);
      auto last2 = std::cend(other.data);
      while (first1 != last1 && first2 != last2) {
        if (first1->first < first2->first) {
          ++first1;
        } else  {
          if (!(first2->first < first1->first)) {
            dbResponse->push_back(DBResponseToString(first1->first, first1->second, first2->second));
            ++first1;
          }
          ++first2;
        }
      }
    }

    // Если блокировка на чтение стала помехой добавления данных, то можно попытаться это сделать
    // по завершению этой функции. Если кто-то еще будет читать данную таблицу, то данная попытка
    // провалиться, но удасться у последнего "читающего".
    TryMoveDefferedData();
    other.TryMoveDefferedData();
  }

  // Не был использован алгоритм из stl, чтобы избежать двойных проходов
  // по коллекциям таблиц из-за невозможности конструирования объекта
  // результата set_operation_result, содержащего конечный результат только
  // лишь из одного итератора на std::map<size_t, std::string>.
  void SymmetricDifference(DataTable& other, std::shared_ptr<DBResponse>& dbResponse) {

    // Оформлено в виде блока, чтобы не писать unlock
    {
      std::shared_lock<std::shared_timed_mutex> lock_read_data(data_mutex, std::defer_lock);
      std::shared_lock<std::shared_timed_mutex> lock_read_other_data(data_mutex, std::defer_lock);
      std::lock(lock_read_data, lock_read_other_data);

      auto first1 = std::cbegin(data);
      auto last1 = std::cend(data);
      auto first2 = std::cbegin(other.data);
      auto last2 = std::cend(other.data);
      while (first1 != last1) {
        if (first2 == last2) {
          while (first1 != last1) {
            dbResponse->push_back(DBResponseToString(first1->first, first1->second, ""));
            ++first1;
          }
          break;
        }

        if (first1->first < first2->first) {
          dbResponse->push_back(DBResponseToString(first1->first, first1->second, ""));
          ++first1;
        }
        else {
          if (first2->first < first1->first) {
            dbResponse->push_back(DBResponseToString(first2->first, "", first2->second));
          } else {
            ++first1;
          }
          ++first2;
        }
      }
      while (first2 != last2) {
        dbResponse->push_back(DBResponseToString(first2->first, "", first2->second));
        ++first2;
      }
    }

    // Если блокировка на чтение стала помехой добавления данных, то можно попытаться это сделать
    // по завершению этой функции. Если кто-то еще будет читать данную таблицу, то данная попытка
    // провалиться, но удасться у последнего "читающего".
    TryMoveDefferedData();
    other.TryMoveDefferedData();
  }

  // Для тестирования работоспособности блокировок
  void PauseInSymmetricDifference(DataTable& other, size_t seconds, std::shared_ptr<DBResponse>& dbResponse) {

    // Оформлено в виде блока, чтобы не писать unlock
    {
      std::shared_lock<std::shared_timed_mutex> lock_read_data(data_mutex, std::defer_lock);
      std::shared_lock<std::shared_timed_mutex> lock_read_other_data(data_mutex, std::defer_lock);
      std::lock(lock_read_data, lock_read_other_data);

      std::this_thread::sleep_for(std::chrono::seconds(seconds));

      auto first1 = std::cbegin(data);
      auto last1 = std::cend(data);
      auto first2 = std::cbegin(other.data);
      auto last2 = std::cend(other.data);
      while (first1 != last1) {
        if (first2 == last2) {
          while (first1 != last1) {
            dbResponse->push_back(DBResponseToString(first1->first, first1->second, ""));
            ++first1;
          }
          break;
        }

        if (first1->first < first2->first) {
          dbResponse->push_back(DBResponseToString(first1->first, first1->second, ""));
          ++first1;
        }
        else {
          if (first2->first < first1->first) {
            dbResponse->push_back(DBResponseToString(first2->first, "", first2->second));
          } else {
            ++first1;
          }
          ++first2;
        }
      }
      while (first2 != last2) {
        dbResponse->push_back(DBResponseToString(first2->first, "", first2->second));
        ++first2;
      }
    }

    // Если блокировка на чтение стала помехой добавления данных, то можно попытаться это сделать
    // по завершению этой функции. Если кто-то еще будет читать данную таблицу, то данная попытка
    // провалиться, но удасться у последнего "читающего".
    TryMoveDefferedData();
    other.TryMoveDefferedData();
  }

private:

  std::shared_timed_mutex data_mutex;
  std::map<size_t, std::string> data;           // данные таблицы

  std::mutex deferred_data_mutex;
  std::map<size_t, std::string> deferred_data;  // отложенные для добавления данные
};

class DataBase
{

  DataBase() {
    auto table = std::make_shared<DataTable>();
    tables["A"] = table;
    table = std::make_shared<DataTable>();
    tables["B"] = table;
  }

  auto FindTable(const std::string& name) {
    auto itr = tables.find(name);
    if(std::cend(tables) == itr)
      throw DataBaseException(ERR_TABLE_NOT_FOUND);
    return itr;
  }

public:

  DataBase(const DataBase&) = delete;
  DataBase& operator=(const DataBase&) = delete;
  DataBase(const DataBase&&) = delete;
  DataBase& operator=(const DataBase&&) = delete;

  static DataBase& Instance()
  {
    static DataBase dataBase;
    return dataBase;
  }
  
  auto GetTable(const std::string& name) {
    auto itr = FindTable(name);
    // Атомарное создание копии умного указателя на таблицу, что
    // позволяет работать с ней, даже если параллельно исполняющийся поток 
    // заменит соответствующий умный указатель в коллекции tables.
    return atomic_load(&itr->second);
  }

  void TruncateTable(const std::string& table_name) {
    auto itr = FindTable(table_name);
    // Атомарная замена хранящегося в коллекции tables умного указателя
    // на таблицу на умный указатель с пустой таблицей.
    atomic_store(&itr->second, std::make_shared<DataTable>());
  }

private:

  // Контейнер заполняется в конструкторе. Указатели на таблицы
  // получаются и заменяются с помощью атомарных операций, поэтому
  // никаких блокировок не используется.
  std::map<std::string, std::shared_ptr<DataTable>> tables;
};
