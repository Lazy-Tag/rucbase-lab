/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // Todo:
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    for (int i = 1; i <= file_handle_->file_hdr_.num_pages; i ++ ) {
        RmPageHandle page_handle = file_handle_->fetch_page_handle(i);
        if (page_handle.page_hdr->num_records) {
            char* bitmap = page_handle.bitmap;
            int slot_no = Bitmap::first_bit(1, bitmap, file_handle->file_hdr_.num_records_per_page);
            rid_ = {i, slot_no};
            break;
        }
    }
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // Todo:
    // 找到文件中下一个存放了记录的非空闲位置，用rid_来指向这个位置
    RmPageHandle page_handle = file_handle_->fetch_page_handle(rid_.page_no);
    char* bitmap = page_handle.bitmap;
    int slot_no = Bitmap::next_bit(1, bitmap, file_handle_->file_hdr_.num_records_per_page, rid_.slot_no);
    if (slot_no == file_handle_->file_hdr_.num_records_per_page) {
        rid_.page_no ++ ;
        if (rid_.page_no == file_handle_->file_hdr_.num_pages) {
            rid_.slot_no = 0;
            return ;
        }
        RmPageHandle now_page_handle = file_handle_->fetch_page_handle(rid_.page_no);
        if (page_handle.page_hdr->num_records) {
            char* now_bitmap = now_page_handle.bitmap;
            int now_slot_no = Bitmap::first_bit(1, now_bitmap, file_handle_->file_hdr_.num_records_per_page);
            rid_.slot_no = now_slot_no;
        }
    } else {
        rid_.slot_no = slot_no;
    }
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // Todo: 修改返回值
    return rid_.page_no == file_handle_->file_hdr_.num_pages;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}