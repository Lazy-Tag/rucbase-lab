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
#include "executor_seq_scan.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public SeqScanExecutor {
   private:
    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    IxIndexHandle *ih_;
    std::vector<Rid> rids;
    int scan = 0;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
                      const std::vector<std::string> &index_col_names, Context *context)
        : SeqScanExecutor(sm_manager, tab_name, conds, context, true) {
        sm_manager_ = sm_manager;
        // index_no_ = index_no;
        index_col_names_ = index_col_names;
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
        ih_ = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names)).get();
        get_rids();
    }

    void beginTuple() override {
        scan = 0;
        if (rids.size()) rid_ = rids[scan ++ ];
        else scan ++ ;
    }

    void nextTuple() override {
        if (!is_end()) rid_ = rids[scan ++ ];
        else scan ++ ;
    }

    bool is_end() const override {
        return scan == rids.size() + 1;
    }

    Rid &rid() override { return rid_; }

    void get_rids() {
        IndexMeta now_idx;
        for (const auto &index : tab_.indexes) {
            bool is_index = true;
            for (const auto &li : index.cols) {
                bool is_match = false;
                for (const auto &ri : index_col_names_)
                    if (ri == li.name) {
                        is_match = true;
                        break;
                    }
                if (!is_match) {
                    is_index = false;
                    break;
                }
            }
            if (is_index) {
                now_idx = index;
                break;
            }
        }

        bool is_eq = true;
        for (const auto &cond : conds_)
            if (cond.op != CompOp::OP_EQ) {
                is_eq = false;
                break;
            }
        if (is_eq) {
            auto key = new char[now_idx.col_tot_len + 1];
            int offset = 0;
            for (const auto &col : now_idx.cols) {
                for (const auto &cond : conds_) {
                    if (cond.lhs_col.col_name == col.name) setKey(col.type, key, cond.rhs_val, offset, col.len);
                }
                offset += col.len;
            }
            ih_->get_value(key, &rids, context_->txn_);
            delete[] key;
        } else {
            auto lk = new char[now_idx.col_tot_len + 1], rk = new char[now_idx.col_tot_len + 1];
            Value minv, maxv;
            minv.is_min = maxv.is_max = true;
            minv.int_val = INT32_MIN, minv.float_val = -1e9, minv.str_val = std::string(20, 0);
            minv.int_val = INT32_MAX, minv.float_val = 1e9, minv.str_val = std::string(20, 127);
            auto meta = tab_.get_col(conds_[0].lhs_col.col_name);
            if (conds_.size() == 1) {
                auto cond = conds_[0];
                auto val = cond.rhs_val;
                switch (cond.op) {
                    case OP_EQ:
                        setKey(meta->type, lk, val, 0, meta->len);
                        setKey(meta->type, rk, val, 0, meta->len);
                        ih_->range_query(lk, rk, &rids, context_->txn_, 1, 1);
                        break;
                    case OP_LT:
                        setKey(meta->type, lk, minv, 0, meta->len);
                        setKey(meta->type, rk, val, 0, meta->len);
                        ih_->range_query(lk, rk, &rids, context_->txn_, 1, 0);
                        break;
                    case OP_LE:
                        setKey(meta->type, lk, minv, 0, meta->len);
                        setKey(meta->type, rk, val, 0, meta->len);
                        ih_->range_query(lk, rk, &rids, context_->txn_, 1, 1);
                        break;
                    case OP_GT:
                        setKey(meta->type, lk, val, 0, meta->len);
                        setKey(meta->type, rk, maxv, 0, meta->len);
                        ih_->range_query(lk, rk, &rids, context_->txn_, 0, 1);
                        break;
                    case OP_GE:
                        setKey(meta->type, lk, val, 0, meta->len);
                        setKey(meta->type, rk, maxv, 0, meta->len);
                        ih_->range_query(lk, rk, &rids, context_->txn_, 1, 1);
                        break;
                }
            } else {
                setKey(meta->type, lk, conds_[0].rhs_val, 0, meta->len);
                setKey(meta->type, rk, conds_[1].rhs_val, 0, meta->len);
                ih_->range_query(lk, rk, &rids, context_->txn_, conds_[0].op == OP_LE, conds_[0].op == OP_GE);
            }
        }
    }

    void setKey(ColType type, char *key, const Value &val, int offset, int len) {
        switch (type) {
            case TYPE_INT:
                memcpy(key + offset, (char *)&val.int_val, len);
                break;
            case TYPE_FLOAT:
                memcpy(key + offset, (char *)&val.float_val, len);
                break;
            case TYPE_STRING:
                memcpy(key + offset, val.str_val.c_str(), len);
                break;
        }
    }
};