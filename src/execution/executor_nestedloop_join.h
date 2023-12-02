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

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;   // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;  // 右儿子节点（需要join的表）
    size_t len_;                               // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                // join后获得的记录的字段

    std::vector<Condition> fed_conds_;  // join条件

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        fed_conds_ = std::move(conds);
    }

    bool satisfyCond(const std::vector<Value> &left, const std::vector<Value> &right) {
        bool flag = true;
        for (const auto &cond : fed_conds_) {
            auto lhs_col = cond.lhs_col;
            auto rhs_col = cond.rhs_col;
            auto rhs_val = cond.rhs_val;
            auto condition = cond.op;
            if (!cond.is_rhs_val) {
                int l_idx = std::distance(cols_.cbegin(), get_col(cols_, lhs_col));
                int r_idx = std::distance(cols_.cbegin(), get_col(cols_, rhs_col)) - left.size();
                switch (condition) {
                    case OP_EQ:
                        flag = compare(left[l_idx], right[r_idx]) == 0;
                        break;
                    case OP_NE:
                        flag = compare(left[l_idx], right[r_idx]) != 0;
                        break;
                    case OP_LT:
                        flag = compare(left[l_idx], right[r_idx]) < 0;
                        break;
                    case OP_LE:
                        flag = compare(left[l_idx], right[r_idx]) <= 0;
                        break;
                    case OP_GT:
                        flag = compare(left[l_idx], right[r_idx]) > 0;
                        break;
                    case OP_GE:
                        flag = compare(left[l_idx], right[r_idx]) >= 0;
                        break;
                }
            } else {
                int index = std::distance(cols_.cbegin(), get_col(cols_, lhs_col));
                switch (condition) {
                    case OP_EQ:
                        flag = compare(left[index], rhs_val) == 0;
                        break;
                    case OP_NE:
                        flag = compare(left[index], rhs_val) != 0;
                        break;
                    case OP_LT:
                        flag = compare(left[index], rhs_val) < 0;
                        break;
                    case OP_LE:
                        flag = compare(left[index], rhs_val) <= 0;
                        break;
                    case OP_GT:
                        flag = compare(left[index], rhs_val) > 0;
                        break;
                    case OP_GE:
                        flag = compare(left[index], rhs_val) >= 0;
                        break;
                }
            }
            if (!flag) return false;
        }
        return true;
    }

    void beginTuple() override {
        left_->beginTuple();
        right_->beginTuple();
    }

    void nextTuple() override {
        do {
            right_->nextTuple();
            if (right_->is_end()) {
                right_->beginTuple();
                left_->nextTuple();
            } else if (left_->is_end())
                break;
        } while (!satisfyCond(left_->constructVal(), right_->constructVal()));
    }

    const std::vector<ColMeta>& cols() const override{
        return cols_;
    }

    bool is_end() const override { return left_->is_end(); }

    std::unique_ptr<RmRecord> Next() override {
        RmRecord rec(len_);
        auto left_values = left_->constructVal();
        auto right_values = right_->constructVal();

        for (size_t i = 0; i < cols_.size(); i++) {
            auto &col = cols_[i];
            if (i < left_values.size()) {
                auto &val = left_values[i];
                if (col.type != val.type) {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
                val.init_raw(col.len);
                memcpy(rec.data + col.offset, val.raw->data, col.len);
            } else {
                auto &val = right_values[i - left_values.size()];
                if (col.type != val.type) {
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
                val.init_raw(col.len);
                memcpy(rec.data + col.offset, val.raw->data, col.len);
            }
        }
        return std::make_unique<RmRecord>(rec);
    }

    std::string getType() { return "NestedLoopJoinExecutor"; };

    Rid &rid() override { return _abstract_rid; }
};