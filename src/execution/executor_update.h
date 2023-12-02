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

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;
    int len_;
    std::unordered_map<std::string, std::pair<int, int>> col_name_to_offset_and_len_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;

        for (const auto& col : tab_.cols) {
            len_ += col.len;
            col_name_to_offset_and_len_[col.name] = {col.offset, col.len};
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        for (const auto &rid : rids_) {
            auto page_handle = fh_->fetch_page_handle(rid.page_no);
            char* buf = new char[len_ + 1];
            memcpy(buf, page_handle.get_slot(rid.slot_no), len_);
            for (const auto &it : set_clauses_) {
                auto lv = it.lhs;
                auto rv = it.rhs;
                auto offset_and_len = col_name_to_offset_and_len_[lv.col_name];
                int len = offset_and_len.second, offset = offset_and_len.first;
                switch (rv.type) {
                    case TYPE_INT:
                        memcpy(buf + offset, std::to_string(rv.int_val).c_str(), sizeof(int));
                        break;
                    case TYPE_FLOAT:
                        memcpy(buf + offset, std::to_string(rv.float_val).c_str(), sizeof(float ));
                        break;
                    case TYPE_STRING:
                        int string_size = rv.str_val.size();
                        memcpy(buf + offset, rv.str_val.c_str(), string_size);
                        buf[offset + string_size] = '\0';
                        break;
                }
            }
            fh_->update_record(rid, buf, context_);
        }
        return nullptr;
    }

    std::string getType() { return "UpdateExecutor"; };

    Rid &rid() override { return _abstract_rid; }
};