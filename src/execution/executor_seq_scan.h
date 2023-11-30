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
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    std::vector<Value> constructVal() {
        auto page_handle = fh_->fetch_page_handle(rid_.page_no);
        char* buf = new char[len_ + 1];
        memcpy(buf, page_handle.get_slot(rid_.slot_no), len_);
        Value val;
        std::vector<Value> vec;
        for (auto col : cols_) {
            char dest[col.len + 1];
            memcpy(dest, buf + col.offset, col.len);
            dest[col.len] = '\0';
            val.type = col.type;
            switch (col.type) {
                case TYPE_INT:
                    val.set_int(*(int*) dest);
                    break;
                case TYPE_FLOAT:
                    val.set_float(*(float*) dest);
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

    int compare(Value& a, Value& b) {
        switch (a.type) {
            case TYPE_INT:
                if (a.int_val > b.int_val) return 1;
                else if (a.int_val == b.int_val) return 0;
                else return -1;
                break;
            case TYPE_FLOAT:
                if (a.float_val > b.float_val) return 1;
                else if (a.float_val == b.float_val) return 0;
                else return -1;
                break;
            case TYPE_STRING:
                if (a.str_val > b.str_val) return 1;
                else if (a.str_val == b.str_val) return 0;
                else return -1;
                break;
        }
    }

    bool satisfyCond() {
        auto values = constructVal();
        bool flag = true;
        for (auto cond : conds_) {
            auto condition = cond.op;
            auto col_name = cond.lhs_col.col_name;
            int index = std::distance(cols_.begin(), std::find_if(cols_.begin(), cols_.end(), [&](ColMeta& col) {
                return col.name == col_name;
            }));
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

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    };

    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void beginTuple() override {
        scan_ = std::make_unique<RmScan>(fh_);
        rid_ = scan_->rid();
        while (!scan_->is_end() && !satisfyCond()) {
            scan_->next();
            if (!scan_->is_end())
                rid_ = scan_->rid();
        }
    }

    /**
     * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void nextTuple() override {
        do {
            scan_->next();
            if (scan_->is_end())
                break;
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

    bool is_end() const override {
        return scan_->is_end();
    }

    Rid &rid() override { return rid_; }
};