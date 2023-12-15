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

class SeqScanExecutor : public AbstractExecutor {
   protected:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同
    TabMeta tab_;
    bool is_read;

    int fd_;
    Rid rid_;
    std::unique_ptr<RecScan> scan_;  // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context, bool read) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fd_ = fh_->GetFd();
        fed_conds_ = conds_;
        is_read = read;
        if (is_read)
            addGapLock(conds_, context_, cols_);
    }

    RmFileHandle *getFileHandle() const override { return fh_; }

    std::vector<Value> constructVal() override {
        char *buf = new char[len_ + 1];
        if (!fh_->getRecord(buf, rid_, context_, len_, is_read))
            throw TransactionAbortException(context_->txn_->get_transaction_id(), AbortReason::LOCK_ON_SHIRINKING);
        Value val;
        std::vector<Value> vec;
        for (const auto &col : cols_) {
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
            vec.emplace_back(val);
        }
        delete[] buf;
        return vec;
    }

    bool satisfyCond() {
        auto values = constructVal();
        bool flag = true;
        for (const auto &cond : conds_) {
            auto condition = cond.op;
            auto col_name = cond.lhs_col.col_name;
            int index = std::distance(cols_.begin(), std::find_if(cols_.begin(), cols_.end(),
                                                                  [&](ColMeta &col) { return col.name == col_name; }));
            switch (condition) {
                case OP_EQ:
                    flag = compare(values[index], cond.rhs_val) == 0;
                    break;
                case OP_NE:
                    flag = compare(values[index], cond.rhs_val) != 0;
                    break;
                case OP_LT:
                    flag = compare(values[index], cond.rhs_val) < 0;
                    break;
                case OP_LE:
                    flag = compare(values[index], cond.rhs_val) <= 0;
                    break;
                case OP_GT:
                    flag = compare(values[index], cond.rhs_val) > 0;
                    break;
                case OP_GE:
                    flag = compare(values[index], cond.rhs_val) >= 0;
                    break;
            }
            if (!flag) return false;
        }

        return true;
    }

    const std::vector<ColMeta> &cols() const override { return cols_; };

    size_t tupleLen() const override { return len_; }

    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void beginTuple() override {
        scan_ = std::make_unique<RmScan>(fh_);
        rid_ = scan_->rid();
        while (!scan_->is_end() && !satisfyCond()) {
            scan_->next();
            if (!scan_->is_end()) rid_ = scan_->rid();
        }
    }

    /**
     * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void nextTuple() override {
        do {
            scan_->next();
            if (scan_->is_end()) break;
            rid_ = scan_->rid();
        } while (!satisfyCond());
    }

    /**
     * @brief 返回下一个满足扫描条件的记录
     *
     * @return std::unique_ptr<RmRecord>
     */
    std::unique_ptr<RmRecord> Next() override {
        RmRecord rec(fh_->get_file_hdr().record_size);
        auto values = constructVal();
        for (size_t i = 0; i < values.size(); i++) {
            auto &col = cols_[i];
            auto &val = values[i];
            if (col.type != val.type) {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }
        return std::make_unique<RmRecord>(rec);
    }

    bool is_end() const override { return scan_->is_end(); }

    std::string getType() { return "SeqScanExecutor"; };

    Rid &rid() override { return rid_; }

    void addGapLock(const std::vector<Condition> &conds, Context *context, const std::vector<ColMeta> &cols) {
        auto &gap_lock = context->lock_mgr_->gap_lock[fd_];
        auto txn_id = context->txn_->get_transaction_id();

        Value minv, maxv;
        minv.is_min = maxv.is_max = true;
        minv.int_val = INT32_MIN, minv.float_val = -1e9, minv.str_val = std::string(20, 0);
        minv.int_val = INT32_MAX, minv.float_val = 1e9, minv.str_val = std::string(20, 127);
        HashMap<std::string, int> is_cond;
        for (const auto &cond : conds) {
            auto condition = cond.op;
            auto col_name = cond.lhs_col.col_name;
            is_cond[col_name] = 1;
            auto &vec = gap_lock[{txn_id, col_name}];
            auto val = cond.rhs_val;
            switch (condition) {
                case OP_EQ:
                    append_lock(vec, val, val);
                    break;
                case OP_NE:
                    append_lock(vec, minv, maxv);
                    break;
                case OP_LT:
                case OP_LE:
                    append_lock(vec, minv, val);
                    break;
                case OP_GT:
                case OP_GE:
                    append_lock(vec, val, maxv);
                    break;
            }
        }

        for (const auto &col : cols) {
            auto col_name = col.name;
            auto &vec = gap_lock[{txn_id, col_name}];
            if (!is_cond[col_name]) append_lock(vec, maxv, minv);
            auto &rg = vec.back();
            rg.type = col.type;
        }
    }

    void append_lock(std::vector<Range> &vec, Value &lv, Value &rv) {
        ColType type;
        if (lv.is_max || lv.is_min)
            type = rv.type;
        else
            type = lv.type;
        Range rg(type);
        switch (type) {
            case TYPE_INT:
                rg.int_lval = lv.int_val;
                rg.int_rval = rv.int_val;
                break;
            case TYPE_FLOAT:
                rg.float_lval = lv.float_val;
                rg.float_rval = rv.float_val;
                break;
            case TYPE_STRING:
                rg.str_lval = lv.str_val;
                rg.str_lval = rv.str_val;
                break;
        }
        vec.emplace_back(rg);
    }
};