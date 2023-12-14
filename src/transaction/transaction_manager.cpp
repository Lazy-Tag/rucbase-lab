/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    if (!txn) {
        txn = new Transaction( ++ next_txn_id_) ;
        txn->set_start_ts( ++ next_timestamp_);
        txn_map[next_txn_id_] = txn;
    }
    
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    if (!txn) return ;
    auto write_set = txn->get_write_set();
    if (!write_set->empty()) {
        for (auto& write_record : *write_set) {
            delete write_record;
        }
        write_set->clear();
    }

    auto lock_set = txn->get_lock_set();
    for (const auto& lock_id : *lock_set) {
        lock_manager_->unlock(txn, lock_id);
    }
    log_manager->flush_log_to_disk();
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    if (!txn) return ;

    auto write_set = txn->get_write_set();
    std::reverse(write_set->begin(), write_set->end());
    auto context = new Context(lock_manager_, log_manager, txn);
    for (const auto& write_record : *write_set) {
        auto type = write_record->GetWriteType();
        auto tab_name = write_record->GetTableName();
        auto fh = sm_manager_->fhs_.at(tab_name).get();
        switch (type) {
            case WType::INSERT_TUPLE:
                fh->delete_record(write_record->GetRid(), context);
                break;
            case WType::DELETE_TUPLE:
                fh->insert_record(write_record->GetRecord().data, context);
                break;
            case WType::UPDATE_TUPLE:
                fh->update_record(write_record->GetRid(), write_record->GetRecord().data, context);
                break;
        }
    }
    delete context;
    write_set->clear();
    auto lock_set = txn->get_lock_set();
    for (const auto& lock_id : *lock_set) {
        lock_manager_->unlock(txn, lock_id);
    }
    log_manager->flush_log_to_disk();
    txn->set_state(TransactionState::ABORTED);
}