/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_index_handle.h"

#include "ix_scan.h"

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::lower_bound(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于等于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式，如顺序遍历、二分查找等；使用ix_compare()函数进行比较
    int l = 0, r = page_hdr->num_key;
    while (l < r) {
        int mid = (l + r) >> 1;
        if (ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_) >= 0) r = mid;
        else l = mid + 1;
    }
    return r;
}

/**
 * @brief 在当前node中查找第一个>target的key_idx
 *
 * @return key_idx，范围为[1,num_key)，如果返回的key_idx=num_key，则表示target大于等于最后一个key
 * @note 注意此处的范围从1开始
 */
int IxNodeHandle::upper_bound(const char *target) const {
    // Todo:
    // 查找当前节点中第一个大于target的key，并返回key的位置给上层
    // 提示: 可以采用多种查找方式：顺序遍历、二分查找等；使用ix_compare()函数进行比较

    int l = 0, r = page_hdr->num_key;
    while (l < r) {
        int mid = (l + r) >> 1;
        if (ix_compare(get_key(mid), target, file_hdr->col_types_, file_hdr->col_lens_) > 0) r = mid;
        else l = mid + 1;
    }
    return r;
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 */
bool IxNodeHandle::leaf_lookup(const char *key, Rid **value) {
    // Todo:
    // 1. 在叶子节点中获取目标key所在位置
    // 2. 判断目标key是否存在
    // 3. 如果存在，获取key对应的Rid，并赋值给传出参数value
    // 提示：可以调用lower_bound()和get_rid()函数。
    int idx = lower_bound(key);
    if (idx != page_hdr->num_key && !ix_compare(key, get_key(idx), file_hdr->col_types_, file_hdr->col_lens_)) {
        *value = get_rid(idx);
        return true;
    }
    return false;
}

/**
 * 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 */
std::pair<page_id_t, int> IxNodeHandle::internal_lookup(const char *key) {
    // Todo:
    // 1. 查找当前非叶子节点中目标key所在孩子节点（子树）的位置
    // 2. 获取该孩子节点（子树）所在页面的编号
    // 3. 返回页面编号
    int idx = upper_bound(key);
    Rid* rid;
    if (idx) rid = get_rid(idx - 1);
    else rid = get_rid(idx);
    return {rid->page_no, idx};
}

/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::insert_pairs(int pos, const char *key, const Rid *rid, int n) {
    // Todo:
    // 1. 判断pos的合法性
    // 2. 通过key获取n个连续键值对的key值，并把n个key值插入到pos位置
    // 3. 通过rid获取n个连续键值对的rid值，并把n个rid值插入到pos位置
    // 4. 更新当前节点的键数量
    if (!page_hdr->num_key || pos <= page_hdr->num_key) {
        auto tot_len = file_hdr->col_tot_len_;
        auto key_len = tot_len * n, last_key_len = tot_len * (page_hdr->num_key - pos);
        auto rid_len = sizeof(Rid) * n, last_rid_len = (page_hdr->num_key - pos) * sizeof(Rid);

        memmove(get_key(pos + n), get_key(pos), last_key_len);
        memmove(get_rid(pos + n), get_rid(pos), last_rid_len);

        auto now_keys = get_key(pos);
        auto now_rids = get_rid(pos);
        memcpy(now_keys, key, key_len);
        memcpy(now_rids, rid, rid_len);

        page_hdr->num_key += n;
    }
}

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 */
std::pair<int, int> IxNodeHandle::insert(const char *key, const Rid &value) {
    // Todo:
    // 1. 查找要插入的键值对应该插入到当前节点的哪个位置
    // 2. 如果key重复则不插入
    // 3. 如果key不重复则插入键值对
    // 4. 返回完成插入操作之后的键值对数量
    int idx = lower_bound(key);
    if (!page_hdr->num_key || ix_compare(get_key(idx), key, file_hdr->col_types_, file_hdr->col_lens_))
        insert_pairs(idx, key, &value, 1);
    return {page_hdr->num_key, idx};
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 */
void IxNodeHandle::erase_pair(int pos) {
    // Todo:
    // 1. 删除该位置的key
    // 2. 删除该位置的rid
    // 3. 更新结点的键值对数量
    auto last_len = page_hdr->num_key - pos - 1;
    memmove(get_key(pos), get_key(pos + 1), last_len * file_hdr->col_tot_len_);
    memmove(get_rid(pos), get_rid(pos + 1), last_len * sizeof(Rid));
    page_hdr->num_key -- ;
}

/**
 * @brief 用于在结点中删除指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return 完成删除操作后的键值对数量
 */
std::pair<int, int> IxNodeHandle::remove(const char *key) {
    // Todo:
    // 1. 查找要删除键值对的位置
    // 2. 如果要删除的键值对存在，删除键值对
    // 3. 返回完成删除操作后的键值对数量
    int idx = lower_bound(key);
    if (idx != page_hdr->num_key && !ix_compare(key, get_key(idx), file_hdr->col_types_, file_hdr->col_lens_))
        erase_pair(idx);
    return {page_hdr->num_key, idx};
}

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    // init file_hdr_
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, (char *)&file_hdr_, sizeof(file_hdr_));
    char* buf = new char[PAGE_SIZE];
    memset(buf, 0, PAGE_SIZE);
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
    file_hdr_ = new IxFileHdr();
    file_hdr_->deserialize(buf);
    
    // disk_manager管理的fd对应的文件中，设置从file_hdr_->num_pages开始分配page_no
    int now_page_no = disk_manager_->get_fd2pageno(fd);
    disk_manager_->set_fd2pageno(fd, now_page_no + 1);

    node_mutex[now_page_no] = new std::shared_mutex();
    node_mutex[IX_LEAF_HEADER_PAGE] = new std::shared_mutex();
    auto leaf_header = fetch_node(IX_LEAF_HEADER_PAGE);
    leaf_header->set_next_leaf(2); leaf_header->set_prev_leaf(2);
    buffer_pool_manager_->unpin_page(leaf_header->get_page_id(), true);
}

/**
 * @brief 用于查找指定键所在的叶子结点
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return [leaf node] and [root_is_latched] 返回目标叶子结点以及根结点是否加锁
 * @note need to Unlatch and unpin the leaf node outside!
 * 注意：用了FindLeafPage之后一定要unlatch叶结点，否则下次latch该结点会堵塞！
 */
IxNodeHandle * IxIndexHandle::find_leaf_page(const char *key, Operation operation, Transaction *transaction) {
    // Todo:
    // 1. 获取根节点
    // 2. 从根节点开始不断向下查找目标key
    // 3. 找到包含该key值的叶子结点停止查找，并返回叶子节点
    auto root = fetch_node(file_hdr_->root_page_);
    auto target = root;
    if (operation == Operation::FIND) {
        read_lock(target);
        if (!target->is_root_page()) {
            buffer_pool_manager_->unpin_page(target->get_page_id(), false);
            target = fetch_node(file_hdr_->root_page_);
            read_unlock(target);
        }
        while (!target->is_leaf_page()) {
            auto parent = target;
            target = fetch_node(target->internal_lookup(key).first);
            read_lock(target);
            read_unlock(parent);
            buffer_pool_manager_->unpin_page(parent->get_page_id(), false);
        }
    } else if (operation == Operation::INSERT) {
        write_lock(target); transaction->append_index_latch_page_set(target->page);
        if (!target->is_root_page()) {
            buffer_pool_manager_->unpin_page(target->get_page_id(), false);
            unlock_ancestor(transaction);
            target = fetch_node(file_hdr_->root_page_);
            write_lock(target); transaction->append_index_latch_page_set(target->page);
        }
        while (!target->is_leaf_page()) {
            auto parent = target;
            auto page_no_and_idx = target->internal_lookup(key);
            auto page_no = page_no_and_idx.first, idx = page_no_and_idx.second;
            target = fetch_node(page_no);

            write_lock(target);
            if (idx && target->page_hdr->num_key < target->get_max_size() - 1) unlock_ancestor(transaction);
            transaction->append_index_latch_page_set(target->page);
            buffer_pool_manager_->unpin_page(parent->get_page_id(), false);
        }
    } else {
        write_lock(target); transaction->append_index_latch_page_set(target->page);
        if (!target->is_root_page()) {
            buffer_pool_manager_->unpin_page(target->get_page_id(), false);
            unlock_ancestor(transaction);
            target = fetch_node(file_hdr_->root_page_);
            write_lock(target); transaction->append_index_latch_page_set(target->page);
        }
        while (!target->is_leaf_page()) {
            auto parent = target;
            auto page_no_and_idx = target->internal_lookup(key);
            auto page_no = page_no_and_idx.first, idx = page_no_and_idx.second;
            target = fetch_node(page_no);

            write_lock(target);
            if (idx && (target->page_hdr->num_key > target->get_min_size() ||
                (target->is_root_page() && target->page_hdr->num_key > 2)))
                    unlock_ancestor(transaction);
            transaction->append_index_latch_page_set(target->page);
            buffer_pool_manager_->unpin_page(parent->get_page_id(), false);
        }
    }

    return target;
}

/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @param transaction 事务指针
 * @return bool 返回目标键值对是否存在
 */
bool IxIndexHandle::get_value(const char *key, std::vector<Rid> *result, Transaction *transaction) {
    // Todo:
    // 1. 获取目标key值所在的叶子结点
    // 2. 在叶子节点中查找目标key值的位置，并读取key对应的rid
    // 3. 把rid存入result参数中.
    // 提示：使用完buffer_pool提供的page之后，记得unpin page；记得处理并发的上锁
    auto leaf_node = find_leaf_page(key, Operation::FIND, transaction);
    Rid **value = new Rid*();
    if (!leaf_node->leaf_lookup(key, value)) {
        read_unlock(leaf_node);
        return false;
    }
    read_unlock(leaf_node);
    result->push_back(**value);
    buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), false);
    return true;
}

/**
 * @brief  将传入的一个node拆分(Split)成两个结点，在node的右边生成一个新结点new node
 * @param node 需要拆分的结点
 * @return 拆分得到的new_node
 * @note need to unpin the new node outside
 * 注意：本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 */
IxNodeHandle *IxIndexHandle::split(IxNodeHandle *node) {
    // Todo:
    // 1. 将原结点的键值对平均分配，右半部分分裂为新的右兄弟结点
    //    需要初始化新节点的page_hdr内容
    // 2. 如果新的右兄弟结点是叶子结点，更新新旧节点的prev_leaf和next_leaf指针
    //    为新节点分配键值对，更新旧节点的键值对数记录
    // 3. 如果新的右兄弟结点不是叶子结点，更新该结点的所有孩子结点的父节点信息(使用IxIndexHandle::maintain_child())
    auto new_node = create_node();
    write_lock(new_node);
    auto& new_handler = new_node->page_hdr;
    auto& handler = node->page_hdr;

    auto nums = handler->num_key;
    int pos = handler->num_key / 2;
    handler->num_key = pos;
    auto insert_key = node->get_key(pos);
    auto insert_rid = node->get_rid(pos);
    new_node->insert_pairs(0, insert_key, insert_rid, nums - pos);

    new_handler->parent = handler->parent;
    new_handler->is_leaf = handler->is_leaf;
    if (new_handler->is_leaf) {
        auto old_next_node = fetch_node(handler->next_leaf);

        write_lock(old_next_node);
        new_handler->prev_leaf = node->get_page_no();
        new_handler->next_leaf = handler->next_leaf;
        handler->next_leaf = new_node->get_page_no();
        old_next_node->set_prev_leaf(new_node->get_page_no());
        buffer_pool_manager_->unpin_page(old_next_node->get_page_id(), true);
        write_unlock(old_next_node);

        if (file_hdr_->last_leaf_ == node->get_page_no())
            file_hdr_->last_leaf_ = new_node->get_page_no();
    } else {
        for (int i = 0; i < new_handler->num_key; i ++ )
            maintain_child(new_node, i);
    }
    return new_node;
}

/**
 * @brief Insert key & value pair into internal page after split
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>=maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 *
 * @param (old_node, new_node) 原结点为old_node，old_node被分裂之后产生了新的右兄弟结点new_node
 * @param key 要插入parent的key
 * @note 一个结点插入了键值对之后需要分裂，分裂后左半部分的键值对保留在原结点，在参数中称为old_node，
 * 右半部分的键值对分裂为新的右兄弟节点，在参数中称为new_node（参考Split函数来理解old_node和new_node）
 * @note 本函数执行完毕后，new node和old node都需要在函数外面进行unpin
 */
void IxIndexHandle::insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node,
                                       Transaction *transaction) {
    // Todo:
    // 1. 分裂前的结点（原结点, old_node）是否为根结点，如果为根结点需要分配新的root
    // 2. 获取原结点（old_node）的父亲结点
    // 3. 获取key对应的rid，并将(key, rid)插入到父亲结点
    // 4. 如果父亲结点仍需要继续分裂，则进行递归插入
    // 提示：记得unpin page

    if (old_node->is_root_page()) {
        auto root_node = create_node();
        write_lock(root_node);
        root_node->page_hdr->parent = INVALID_PAGE_ID;
        root_node->insert_pair(0, old_node->get_key(0), {old_node->get_page_no(), 0});
        root_node->insert_pair(1, new_node->get_key(0), {new_node->get_page_no(), 0});

        maintain_child(root_node, 0); maintain_child(root_node, 1);
        update_root_page_no(root_node->get_page_no());

        buffer_pool_manager_->unpin_page(root_node->get_page_id(), true);
        write_unlock(root_node);
        write_unlock(old_node); write_unlock(new_node);
        transaction->pop_index_latch_page_set();
    } else {
        auto parent_node = fetch_node(old_node->page_hdr->parent);
        int nums = parent_node->insert(key, {new_node->get_page_no(), 0}).first;
        write_unlock(old_node); write_unlock(new_node);
        transaction->pop_index_latch_page_set();
        if (nums == parent_node->get_max_size()) {
            auto next_new_node = split(parent_node);
            auto new_key = next_new_node->get_key(0);
            insert_into_parent(parent_node, new_key, next_new_node, transaction);

            buffer_pool_manager_->unpin_page(parent_node->get_page_id(), true);
            buffer_pool_manager_->unpin_page(next_new_node->get_page_id(), true);
        } else {
            buffer_pool_manager_->unpin_page(parent_node->get_page_id(), true);
            unlock_ancestor(transaction);
        }
    }
}

/**
 * @brief 将指定键值对插入到B+树中
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return page_id_t 插入到的叶结点的page_no
 */
page_id_t IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) {
    // Todo:
    // 1. 查找key值应该插入到哪个叶子节点
    // 2. 在该叶子节点中插入键值对
    // 3. 如果结点已满，分裂结点，并把新结点的相关信息插入父节点
    // 提示：记得unpin page；若当前叶子节点是最右叶子节点，则需要更新file_hdr_.last_leaf；记得处理并发的上锁
    auto leaf_node = find_leaf_page(key, Operation::INSERT, transaction);
    auto nums_and_idx = leaf_node->insert(key, value);
    int nums = nums_and_idx.first, idx = nums_and_idx.second;
    if (!idx)
        maintain_parent(leaf_node);

    if (nums == leaf_node->get_max_size()) {
        auto new_node = split(leaf_node);
        auto new_key = new_node->get_key(0);
        insert_into_parent(leaf_node, new_key, new_node, transaction);
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), true);
        buffer_pool_manager_->unpin_page(new_node->get_page_id(), true);
    } else {
        unlock_ancestor(transaction);
        buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), true);
    }

    return leaf_node->get_page_no();
}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 * @param key 要删除的key值
 * @param transaction 事务指针
 */
bool IxIndexHandle::delete_entry(const char *key, Transaction *transaction) {
    // Todo:
    // 1. 获取该键值对所在的叶子结点
    // 2. 在该叶子结点中删除键值对
    // 3. 如果删除成功需要调用CoalesceOrRedistribute来进行合并或重分配操作，并根据函数返回结果判断是否有结点需要删除
    // 4. 如果需要并发，并且需要删除叶子结点，则需要在事务的delete_page_set中添加删除结点的对应页面；记得处理并发的上锁
    auto leaf_node = find_leaf_page(key, Operation::DELETE, transaction);
    int old_nums = leaf_node->page_hdr->num_key;
    auto nums_and_idx = leaf_node->remove(key);

    if (nums_and_idx.first == old_nums) {
        unlock_ancestor(transaction);
        return false;
    }

    if (!nums_and_idx.second)
        maintain_parent(leaf_node);

    bool is_delete = coalesce_or_redistribute(leaf_node, transaction);
    if (!is_delete) buffer_pool_manager_->unpin_page(leaf_node->get_page_id(), true);
    return true;
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * @param node 执行完删除操作的结点
 * @param transaction 事务指针
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点
 * @note User needs to first find the sibling of input page.
 * If sibling's size + input page's size >= 2 * page's minsize, then redistribute.
 * Otherwise, merge(Coalesce).
 */
bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction) {
    // Todo:
    // 1. 判断node结点是否为根节点
    //    1.1 如果是根节点，需要调用adjust_root() 函数来进行处理，返回根节点是否需要被删除
    //    1.2 如果不是根节点，并且不需要执行合并或重分配操作，则直接返回false，否则执行2
    // 2. 获取node结点的父亲结点
    // 3. 寻找node结点的兄弟结点（优先选取前驱结点）
    // 4. 如果node结点和兄弟结点的键值对数量之和，能够支撑两个B+树结点（即node.size+neighbor.size >=
    // NodeMinSize*2)，则只需要重新分配键值对（调用Redistribute函数）
    // 5. 如果不满足上述条件，则需要合并两个结点，将右边的结点合并到左边的结点（调用Coalesce函数）
    if (node->is_root_page()) {
        return adjust_root(node, transaction);
    } else if (node->page_hdr->num_key >= node->get_min_size()) {
        unlock_ancestor(transaction);
        return false;
    }

    auto parent_node = fetch_node(node->page_hdr->parent);
    IxNodeHandle *neighbor_node;
    char* key = node->get_key(0);
    int idx = parent_node->lower_bound(key);
    if (!idx) {
        neighbor_node = fetch_node(parent_node->get_rid(idx + 1)->page_no);
    } else {
        neighbor_node = fetch_node(parent_node->get_rid(idx - 1)->page_no);
    }

    write_lock(neighbor_node);
    int tot_num = node->page_hdr->num_key + neighbor_node->page_hdr->num_key;
    if (tot_num >= node->get_min_size() * 2) {
        redistribute(neighbor_node, node, parent_node, idx);
        buffer_pool_manager_->unpin_page(neighbor_node->get_page_id(), true);
        buffer_pool_manager_->unpin_page(parent_node->get_page_id(), true);
        unlock_ancestor(transaction);
        return false;
    } else {
        bool is_delete_parent = coalesce(&neighbor_node, &node, &parent_node, idx, transaction);
        buffer_pool_manager_->unpin_page(neighbor_node->get_page_id(), true);
        if (!is_delete_parent) buffer_pool_manager_->unpin_page(parent_node->get_page_id(), true);
        return true;
    }
}

/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note size of root page can be less than min size and this method is only called within coalesce_or_redistribute()
 */
bool IxIndexHandle::adjust_root(IxNodeHandle *old_root_node, Transaction* transaction) {
    // Todo:
    // 1. 如果old_root_node是内部结点，并且大小为1，则直接把它的孩子更新成新的根结点
    // 2. 如果old_root_node是叶结点，且大小为0，则直接更新root page
    // 3. 除了上述两种情况，不需要进行操作

    if (old_root_node->is_leaf_page()) {
        if (!old_root_node->page_hdr->num_key) {
            unlock_ancestor(transaction);
            return true;
        }
    } else {
        if (old_root_node->page_hdr->num_key == 1) {
            auto new_root = fetch_node(old_root_node->get_rid(0)->page_no);
            write_lock(new_root);
            update_root_page_no(new_root->get_page_no());
            new_root->page_hdr->parent = INVALID_PAGE_ID;
            buffer_pool_manager_->unpin_page(new_root->get_page_id(), true);
            write_unlock(new_root);
            unlock_ancestor(transaction);
            return true;
        }
    }
    unlock_ancestor(transaction);
    return false;
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 * Redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
 * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
 *
 * @param neighbor_node sibling page of input "node"
 * @param node input from method coalesceOrRedistribute()
 * @param parent the parent of "node" and "neighbor_node"
 * @param index node在parent中的rid_idx
 * @note node是之前刚被删除过一个key的结点
 * index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
 * index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
 * 注意更新parent结点的相关kv对
 */
void IxIndexHandle::redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
    // Todo:
    // 1. 通过index判断neighbor_node是否为node的前驱结点
    // 2. 从neighbor_node中移动一个键值对到node结点中
    // 3. 更新父节点中的相关信息，并且修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
    // 注意：neighbor_node的位置不同，需要移动的键值对不同，需要分类讨论
    int neighbor_nums = neighbor_node->page_hdr->num_key, node_nums = node->page_hdr->num_key;
    if (index) {
        auto key = neighbor_node->get_key(neighbor_nums - 1);
        auto rid = neighbor_node->get_rid(neighbor_nums - 1);
        node->insert_pair(0, key, *rid);
        neighbor_node->erase_pair(neighbor_nums - 1);
        parent->set_key(index, key);
    } else {
        auto key = neighbor_node->get_key(0);
        auto rid = neighbor_node->get_rid(0);
        node->insert_pair(node_nums, key, *rid);
        neighbor_node->erase_pair(0);
        parent->set_key(index + 1, neighbor_node->get_key(0));
    }
    write_unlock(neighbor_node);
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 */
bool IxIndexHandle::coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                             Transaction *transaction) {
    // Todo:
    // 1. 用index判断neighbor_node是否为node的前驱结点，若不是则交换两个结点，让neighbor_node作为左结点，node作为右结点
    // 2. 把node结点的键值对移动到neighbor_node中，并更新node结点孩子结点的父节点信息（调用maintain_child函数）
    // 3. 释放和删除node结点，并删除parent中node结点的信息，返回parent是否需要被删除
    // 提示：如果是叶子结点且为最右叶子结点，需要更新file_hdr_.last_leaf

    if (!index) std::swap(node, neighbor_node);
    int neighbor_nums = (*neighbor_node)->page_hdr->num_key, node_nums = (*node)->page_hdr->num_key;
    (*neighbor_node)->insert_pairs(neighbor_nums, (*node)->get_key(0), (*node)->get_rid(0), node_nums);
    for (int i = neighbor_nums; i < neighbor_nums + node_nums; i ++ )
        maintain_child(*neighbor_node, i);
    if ((*node)->is_leaf_page() && (*node)->get_page_no() == file_hdr_->last_leaf_)
        file_hdr_->last_leaf_ = (*neighbor_node)->page_hdr->prev_leaf;
    release_node_handle(**node);

    write_unlock(*neighbor_node);
    write_unlock(*node); transaction->pop_index_latch_page_set();
    if (index) (*parent)->erase_pair(index);
    else (*parent)->erase_pair(index + 1);
    file_hdr_->num_pages_ -- ;
    return coalesce_or_redistribute(*parent, transaction);
}

/**
 * @brief 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
 * node其实就是把slot_no作为键值对数组的下标
 * 换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
 *
 * @param iid
 * @return Rid
 * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
 */
Rid IxIndexHandle::get_rid(const Iid &iid) const {
    IxNodeHandle *node = fetch_node(iid.page_no);
    if (iid.slot_no >= node->get_size()) {
        auto next_node = fetch_node(node->get_next_leaf());
        buffer_pool_manager_->unpin_page(next_node->get_page_id(), false);
        buffer_pool_manager_->unpin_page(node->get_page_id(), false);
        return *next_node->get_rid(0);
    }
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);  // unpin it!
    return *node->get_rid(iid.slot_no);
}

/**
 * @brief FindLeafPage + lower_bound
 *
 * @param key
 * @return Iid
 * @note 上层传入的key本来是int类型，通过(const char *)&key进行了转换
 * 可用*(int *)key转换回去
 */
Iid IxIndexHandle::lower_bound(const char *key, Transaction* transaction) {
    auto leaf_node = find_leaf_page(key, Operation::FIND, transaction);
    unlock_ancestor(transaction);
    int idx = leaf_node->lower_bound(key);
    return {leaf_node->get_page_no(), idx};
}

/**
 * @brief FindLeafPage + upper_bound
 *
 * @param key
 * @return Iid
 */
Iid IxIndexHandle::upper_bound(const char *key, Transaction* transaction) {
    auto leaf_node = find_leaf_page(key, Operation::FIND, transaction);
    unlock_ancestor(transaction);
    int idx = leaf_node->upper_bound(key);
    return {leaf_node->get_page_no(), idx};
}

/**
 * @brief 指向最后一个叶子的最后一个结点的后一个
 * 用处在于可以作为IxScan的最后一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_end() const {
    IxNodeHandle *node = fetch_node(file_hdr_->last_leaf_);
    Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->get_size()};
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);  // unpin it!
    return iid;
}

/**
 * @brief 指向第一个叶子的第一个结点
 * 用处在于可以作为IxScan的第一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_begin() const {
    Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
    return iid;
}

/**
 * @brief 获取一个指定结点
 *
 * @param page_no
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 */
IxNodeHandle *IxIndexHandle::fetch_node(int page_no) const {
    Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    IxNodeHandle *node = new IxNodeHandle(file_hdr_, page);
    return node;
}

/**
 * @brief 创建一个新结点
 *
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create node，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxNodeHandle *IxIndexHandle::create_node() {
    IxNodeHandle *node;
    file_hdr_->num_pages_++;

    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    node = new IxNodeHandle(file_hdr_, page);
    node_mutex[page->get_page_id().page_no] = new std::shared_mutex();
    return node;
}

/**
 * @brief 从node开始更新其父节点的第一个key，一直向上更新直到根节点
 *
 * @param node
 */
void IxIndexHandle::maintain_parent(IxNodeHandle *node) {
    IxNodeHandle *curr = node;
    while (curr->get_parent_page_no() != IX_NO_PAGE) {
        // Load its parent
        IxNodeHandle *parent = fetch_node(curr->get_parent_page_no());
        int rank = parent->find_child(curr);
        char *parent_key = parent->get_key(rank);
        char *child_first_key = curr->get_key(0);
        if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0) {
            assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
            break;
        }
        memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_);  // 修改了parent node
        curr = parent;

        assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
    }
}

/**
 * @brief 要删除leaf之前调用此函数，更新leaf前驱结点的next指针和后继结点的prev指针
 *
 * @param leaf 要删除的leaf
 */
void IxIndexHandle::erase_leaf(IxNodeHandle *leaf) {
    assert(leaf->is_leaf_page());

    IxNodeHandle *prev = fetch_node(leaf->get_prev_leaf());
    prev->set_next_leaf(leaf->get_next_leaf());
    buffer_pool_manager_->unpin_page(prev->get_page_id(), true);

    IxNodeHandle *next = fetch_node(leaf->get_next_leaf());
    next->set_prev_leaf(leaf->get_prev_leaf());  // 注意此处是SetPrevLeaf()
    buffer_pool_manager_->unpin_page(next->get_page_id(), true);
}

/**
 * @brief 删除node时，更新file_hdr_.num_pages
 *
 * @param node
 */
void IxIndexHandle::release_node_handle(IxNodeHandle &node) {
    if (node.is_leaf_page()) erase_leaf(&node);
    file_hdr_->num_pages_--;
    buffer_pool_manager_->delete_page(node.get_page_id());
}

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 */
void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx) {
    if (!node->is_leaf_page()) {
        //  Current node is inner node, load its child and set its parent to current node
        int child_page_no = node->value_at(child_idx);
        IxNodeHandle *child = fetch_node(child_page_no);
        child->set_parent_page_no(node->get_page_no());
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
    }
}

/**
 * @brief 将事务中所有祖先解锁
 */

void IxIndexHandle::unlock_ancestor(Transaction *transaction) {
    auto locked_page = transaction->get_index_latch_page_set();
    for (auto page : *locked_page) {
        auto page_id = page->get_page_id();
        auto node = fetch_node(page_id.page_no);
        write_unlock(node);
        buffer_pool_manager_->unpin_page(page_id, false);
    }
    transaction->clear_index_latch_page_set();
}