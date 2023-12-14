/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <condition_variable>
#include <mutex>
#include <set>

#include "index/ix_index_handle.h"
#include "transaction/transaction.h"

static const std::string GroupLockModeStr[10] = {"NON_LOCK", "IS", "IX", "S", "X", "SIX"};

template <typename T>
class Set {
   private:
    std::set<T> set_;
    std::mutex mtx;

   public:
    void insert(const T& value) {
        std::lock_guard<std::mutex> guard(mtx);
        set_.insert(value);
    }

    void erase(const T& value) {
        std::lock_guard<std::mutex> guard(mtx);
        set_.erase(value);
    }

    typename std::set<T>::reverse_iterator rbegin() {
        std::lock_guard<std::mutex> guard(mtx);
        return set_.rbegin();
    }

    size_t size() {
        std::lock_guard<std::mutex> guard(mtx);
        return set_.size();
    }
};

class LockManager {
    /* 加锁类型，包括共享锁、排他锁、意向共享锁、意向排他锁、SIX（意向排他锁+共享锁） */
    enum class LockMode { SHARED, EXLUCSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, S_IX };

    /* 用于标识加锁队列中排他性最强的锁类型，例如加锁队列中有SHARED和EXLUSIVE两个加锁操作，则该队列的锁模式为X */
    enum class TableLockMode { NON_LOCK, IS, S, IX, X, SIX };

    /* 数据项上的加锁队列 */
    class TableModeSet {
       public:
        Set<TableLockMode> mode_set;  // 加锁队列
        std::condition_variable cv_;  // 条件变量，用于唤醒正在等待加锁的申请，在no-wait策略下无需使用
        TableLockMode mode_ = TableLockMode::NON_LOCK;  // 加锁队列的锁模式
    };

   public:
    LockManager() {}

    ~LockManager() {}

    bool lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    bool lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd);

    bool lock_shared_on_table(Transaction* txn, int tab_fd);

    bool lock_exclusive_on_table(Transaction* txn, int tab_fd);

    bool lock_IS_on_table(Transaction* txn, int tab_fd);

    bool lock_IX_on_table(Transaction* txn, int tab_fd);

    bool unlock(Transaction* txn, LockDataId lock_data_id);

   private:
    HashMap<LockDataId, std::pair<std::shared_mutex*, txn_id_t>> lock_table_;  // 全局锁表
    HashMap<LockDataId, TableLockMode> lock_mode_table_;
    HashMap<int, TableModeSet> tab_mode_table_;
};