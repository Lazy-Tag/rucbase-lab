/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    auto lock_id = LockDataId(tab_fd, rid, LockDataType::RECORD);
    auto mtx = lock_table_[lock_id].first;
    auto txn_id = lock_table_[lock_id].second;
    if (!mtx) {
        mtx = new std::shared_mutex();
        txn_id = txn->get_transaction_id();
        lock_table_[lock_id] = {mtx, txn_id};
    }

    lock_IS_on_table(txn, tab_fd);
    lock_mode_table_[lock_id] = TableLockMode::IS;

    bool flag = mtx->try_lock_shared();
    if (flag) txn->append_lock_set(lock_id);
    return flag || txn->get_transaction_id() == txn_id;
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    auto lock_id = LockDataId(tab_fd, rid, LockDataType::RECORD);
    auto mtx = lock_table_[lock_id].first;
    auto txn_id = lock_table_[lock_id].second;
    if (!mtx) {
        mtx = new std::shared_mutex();
        txn_id = txn->get_transaction_id();
        lock_table_[lock_id] = {mtx, txn_id};
    }

    lock_IX_on_table(txn, tab_fd);
    lock_mode_table_[lock_id] = TableLockMode::IX;

    bool flag = mtx->try_lock();
    if (txn->get_transaction_id() == txn_id) {
        if (!flag) {
            lock_table_.erase(lock_id);
            auto new_mtx = new std::shared_mutex();
            lock_table_[lock_id] = {new_mtx, txn_id};
            return new_mtx->try_lock();
        } else {
            txn->append_lock_set(lock_id);
            return true;
        }
    } else {
        if (flag) txn->append_lock_set(lock_id);
        return flag;
    }
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    auto lock_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto mtx = lock_table_[lock_id].first;
    auto txn_id = lock_table_[lock_id].second;
    if (!mtx) {
        mtx = new std::shared_mutex();
        txn_id = txn->get_transaction_id();
        lock_table_[lock_id] = {mtx, txn_id};
    }

    txn->append_lock_set(lock_id);
    auto& tab_mode = tab_mode_table_[tab_fd];
    bool flag = tab_mode.mode_ == TableLockMode::S && mtx->try_lock_shared();

    txn->append_lock_set(lock_id);
    tab_mode.mode_set.insert(TableLockMode::S);
    tab_mode.mode_ = *tab_mode.mode_set.rbegin();
    lock_mode_table_[lock_id] = TableLockMode::S;

    return flag;
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    auto lock_id = LockDataId(tab_fd, LockDataType::TABLE);
    auto mtx = lock_table_[lock_id].first;
    auto txn_id = lock_table_[lock_id].second;
    if (!mtx) {
        mtx = new std::shared_mutex();
        txn_id = txn->get_transaction_id();
        lock_table_[lock_id] = {mtx, txn_id};
    }

    txn->append_lock_set(lock_id);
    auto& tab_mode = tab_mode_table_[tab_fd];
    bool flag = tab_mode.mode_ == TableLockMode::NON_LOCK && mtx->try_lock();

    txn->append_lock_set(lock_id);
    tab_mode.mode_set.insert(TableLockMode::X);
    tab_mode.mode_ = *tab_mode.mode_set.rbegin();
    lock_mode_table_[lock_id] = TableLockMode::X;

    return flag;
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    auto& tab_mode = tab_mode_table_[tab_fd];
    tab_mode.mode_set.insert(TableLockMode::IS);
    tab_mode.mode_ = *tab_mode.mode_set.rbegin();
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    auto& tab_mode = tab_mode_table_[tab_fd];
    tab_mode.mode_set.insert(TableLockMode::IX);
    tab_mode.mode_ = *tab_mode.mode_set.rbegin();
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    auto mtx = lock_table_[lock_data_id].first;
    auto tab_fd = lock_data_id.fd_;
    auto& tab_mode = tab_mode_table_[tab_fd];
    tab_mode.mode_set.erase(lock_mode_table_[lock_data_id]);
    if (tab_mode.mode_set.size())
        tab_mode.mode_ = *tab_mode.mode_set.rbegin();
    else
        tab_mode.mode_ = TableLockMode::NON_LOCK;
    if (mtx) lock_table_.erase(lock_data_id);
    return true;
}