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

std::unordered_map<txn_id_t, Transaction*> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction* TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    if (!txn) {
        txn = new Transaction(++next_txn_id_);
        txn->set_start_ts(++next_timestamp_);
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
    if (!txn) return;
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
    eraseGapLock(txn);
    log_manager->flush_log_to_disk();
    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    if (!txn) return;

    auto write_set = txn->get_write_set();
    std::reverse(write_set->begin(), write_set->end());
    auto context = new Context(lock_manager_, log_manager, txn);
    for (const auto& write_record : *write_set) {
        auto type = write_record->GetWriteType();
        auto tab_name = write_record->GetTableName();
        auto fh = sm_manager_->fhs_.at(tab_name).get();
        auto tab = sm_manager_->db_.get_table(write_record->GetTableName());
        auto buf = write_record->GetRecord().data;
        int len = tab.cols.back().offset + tab.cols.back().len;
        char old_buf[len + 1];
        fh->getRecord(old_buf, write_record->GetRid(), context, len, false);
        switch (type) {
            case WType::INSERT_TUPLE: {
                fh->delete_record(write_record->GetRid(), context);
                for (size_t i = 0; i < tab.indexes.size(); ++i) {
                    auto& index = tab.indexes[i];
                    auto ih =
                        sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab.name, index.cols)).get();
                    char* key = new char[index.col_tot_len];
                    int offset = 0;
                    for (size_t j = 0; j < index.col_num; ++j) {
                        memcpy(key + offset, buf + index.cols[j].offset, index.cols[j].len);
                        offset += index.cols[j].len;
                    }
                    ih->delete_entry(key, txn);
                }
                break;
            }
            case WType::DELETE_TUPLE: {
                fh->insert_record(write_record->GetRecord().data, context);
                for (size_t i = 0; i < tab.indexes.size(); ++i) {
                    auto& index = tab.indexes[i];
                    auto ih =
                        sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab.name, index.cols)).get();
                    char* key = new char[index.col_tot_len];
                    int offset = 0;
                    for (size_t j = 0; j < index.col_num; ++j) {
                        memcpy(key + offset, old_buf + index.cols[j].offset, index.cols[j].len);
                        offset += index.cols[j].len;
                    }
                    ih->insert_entry(key, write_record->GetRid(), txn);
                }
                break;
            }
            case WType::UPDATE_TUPLE: {
                for (size_t i = 0; i < tab.indexes.size(); ++i) {
                    auto& index = tab.indexes[i];
                    auto ih =
                        sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab.name, index.cols)).get();
                    char* key = new char[index.col_tot_len];
                    int offset = 0;
                    for (size_t j = 0; j < index.col_num; ++j) {
                        memcpy(key + offset, buf + index.cols[j].offset, index.cols[j].len);
                        offset += index.cols[j].len;
                    }
                    ih->delete_entry(key, txn);
                }
                fh->update_record(write_record->GetRid(), write_record->GetRecord().data, context);
                for (size_t i = 0; i < tab.indexes.size(); ++i) {
                    auto& index = tab.indexes[i];
                    auto ih =
                        sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab.name, index.cols)).get();
                    char* key = new char[index.col_tot_len];
                    int offset = 0;
                    for (size_t j = 0; j < index.col_num; ++j) {
                        memcpy(key + offset, old_buf + index.cols[j].offset, index.cols[j].len);
                        offset += index.cols[j].len;
                    }
                    ih->delete_entry(key, txn);
                }
                break;
            }
        }
    }
    delete context;
    write_set->clear();
    auto lock_set = txn->get_lock_set();
    for (const auto& lock_id : *lock_set) {
        lock_manager_->unlock(txn, lock_id);
    }
    eraseGapLock(txn);
    log_manager->flush_log_to_disk();
    txn->set_state(TransactionState::ABORTED);
}

void TransactionManager::eraseGapLock(Transaction* txn) {
    for (auto& it : lock_manager_->gap_lock) {
        auto is_txn = [&txn](const std::pair<std::pair<txn_id_t, std::string>, std::vector<Range>>& id) {
            return id.first.first == txn->get_transaction_id();
        };
        auto iter = it.second.find_if(is_txn);
        while (iter != it.second.end()) {
            it.second.erase(iter);
            iter = it.second.find_if(is_txn);
        }
    }
}