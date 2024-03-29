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
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;
    int len_ = 0;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
        for (const auto &col : tab_.cols) len_ += col.len;
    }

    std::unique_ptr<RmRecord> Next() override {
        char *buf = new char[len_ + 1];
        for (const auto &rid : rids_) {
            auto page_handle = fh_->fetch_page_handle(rid.page_no);
            memcpy(buf, page_handle.get_slot(rid.slot_no), len_);
            auto record = RmRecord(len_, buf);

            std::vector<Value> values;
            for (const auto &col : tab_.cols) {
                Value val;
                char dest[col.len + 1];
                memcpy(dest, buf + col.offset, col.len);
                dest[col.len] = '\0';
                val.type = col.type;
                switch (col.type) {
                    case TYPE_INT:
                        val.set_int(*(int *)dest);
                        break;
                    case TYPE_FLOAT:
                        val.set_float(*(float *)dest);
                        break;
                    case TYPE_STRING:
                        val.set_str(dest);
                        break;
                }
                values.emplace_back(val);
            }
            if (!fh_->checkGapLock(tab_.cols, values, context_) || !fh_->delete_record(rid, context_)) {
                delete[] buf;
                throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
            }

            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto &index = tab_.indexes[i];
                auto ih =
                    sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char *key = new char[index.col_tot_len];
                int offset = 0;
                for (size_t j = 0; j < index.col_num; ++j) {
                    memcpy(key + offset, buf + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                ih->delete_entry(key, context_->txn_);
            }

            auto write_record = new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, record);
            context_->txn_->append_write_record(write_record);
        }
        delete[] buf;
        return nullptr;
    }

    std::string getType() { return "DeleteExecutor"; };

    Rid &rid() override { return _abstract_rid; }
};