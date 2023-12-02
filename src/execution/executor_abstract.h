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
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor {
   public:
    Rid _abstract_rid;

    Context *context_;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; }

    virtual const std::vector<ColMeta> &cols() const {
        return std::vector<ColMeta>();
    }

    virtual RmFileHandle* getFileHandle() const {}

    virtual std::string getType() { return "AbstractExecutor"; };

    virtual void beginTuple(){};

    virtual void nextTuple(){};

    virtual bool is_end() const {
        return true;
    }

    virtual std::vector<Value> constructVal() {}

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta();};

    std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

    static int compare(const Value &a, const Value &b) {
        switch (a.type) {
            case TYPE_INT:
                if (a.int_val > b.int_val)
                    return 1;
                else if (a.int_val == b.int_val)
                    return 0;
                else
                    return -1;
                break;
            case TYPE_FLOAT:
                if (a.float_val > b.float_val)
                    return 1;
                else if (a.float_val == b.float_val)
                    return 0;
                else
                    return -1;
                break;
            case TYPE_STRING:
                if (a.str_val > b.str_val)
                    return 1;
                else if (a.str_val == b.str_val)
                    return 0;
                else
                    return -1;
                break;
        }
    }
};