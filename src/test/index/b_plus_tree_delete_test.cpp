#include <algorithm>
#include <cstdio>
#include <random>  // for std::default_random_engine

#include "gtest/gtest.h"

#define private public
#include "index/ix.h"
#undef private  // for use private variables in "ix.h"

#include "storage/buffer_pool_manager.h"
#include "system/sm.h"
#include "record/rm.h"
const std::string TEST_DB_NAME = "BPlusTreeInsertTest_db";  // 以数据库名作为根目录
const std::string TEST_FILE_NAME = "table1";                // 测试文件名的前缀
// const int index_no = 0;                                     // 索引编号
const std::vector<std::string> TEST_COL = {"col1"};
// 创建的索引文件名为"table1.0.idx"（TEST_FILE_NAME + index_no + .idx）

/** 注意：每个测试点只测试了单个文件！
 * 对于每个测试点，先创建和进入目录TEST_DB_NAME
 * 然后在此目录下创建和打开索引文件"table1.0.idx"，记录IxIndexHandle */

// Add by jiawen
class BPlusTreeTests : public ::testing::Test {
   public:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<IxIndexHandle> ih_;
    std::unique_ptr<Transaction> txn_;
    std::unique_ptr<RmManager> rm_;
    std::unique_ptr<SmManager> sm_;

   public:
    // This function is called before every test.
    void SetUp() override {
        ::testing::Test::SetUp();
        // For each test, we create a new IxManager
        disk_manager_ = std::make_unique<DiskManager>();
        buffer_pool_manager_ = std::make_unique<BufferPoolManager>(200, disk_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), buffer_pool_manager_.get());
        txn_ = std::make_unique<Transaction>(0);
        rm_ = std::make_unique<RmManager>(disk_manager_.get(), buffer_pool_manager_.get());
        sm_ = std::make_unique<SmManager>(disk_manager_.get(), buffer_pool_manager_.get(), rm_.get(), ix_manager_.get());

        // 如果测试目录不存在，则先创建测试目录
        if (disk_manager_->is_dir(TEST_DB_NAME)) {
            std::string cmd = "rm -rf " + TEST_DB_NAME;
            if (system(cmd.c_str()) < 0) {  
                throw UnixError();
            }
        }
        sm_->create_db(TEST_DB_NAME);
<<<<<<< HEAD
         assert(disk_manager_->is_dir(TEST_DB_NAME));
        // 进入测试目录
         if (chdir(TEST_DB_NAME.c_str()) < 0) {
             throw UnixError();
         }
        // 如果测试文件存在，则先删除原文件（最后留下来的文件存的是最后一个测试点的数据）
         if (ix_manager_->exists(TEST_FILE_NAME, TEST_COL)) {
             ix_manager_->destroy_index(TEST_FILE_NAME, TEST_COL);
         }
=======
        assert(disk_manager_->is_dir(TEST_DB_NAME));
        // 进入测试目录
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
        // 如果测试文件存在，则先删除原文件（最后留下来的文件存的是最后一个测试点的数据）
        if (ix_manager_->exists(TEST_FILE_NAME, TEST_COL)) {
            ix_manager_->destroy_index(TEST_FILE_NAME, TEST_COL);
        }
>>>>>>> 472ca4f492a7f0678a663e7a959de5889f67b859
        std::vector<ColDef> coldef;
        coldef.push_back({"col1", TYPE_INT, 4});
        coldef.push_back({"col2", TYPE_INT, 4});
        sm_->create_table(TEST_FILE_NAME, coldef, nullptr);
        sm_->create_index(TEST_FILE_NAME, TEST_COL, nullptr);
        assert(ix_manager_->exists(TEST_FILE_NAME, TEST_COL));
        // 打开测试文件
        ih_ = ix_manager_->open_index(TEST_FILE_NAME, TEST_COL);
        assert(ih_ != nullptr);
    }

    // This function is called after every test.
    void TearDown() override {
        ix_manager_->close_index(ih_.get());
        // ix_manager_->destroy_index(TEST_FILE_NAME, index_no);  // 若不删除数据库文件，则将保留最后一个测试点的数据

        // 返回上一层目录
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
    };

    void ToGraph(const IxIndexHandle *ih, IxNodeHandle *node, BufferPoolManager *bpm, std::ofstream &out) const {
        std::string leaf_prefix("LEAF_");
        std::string internal_prefix("INT_");
        if (node->is_leaf_page()) {
            IxNodeHandle *leaf = node;
            // Print node name
            out << leaf_prefix << leaf->get_page_no();
            // Print node properties
            out << "[shape=plain color=green ";
            // Print data of the node
            out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
            // Print data
            out << "<TR><TD COLSPAN=\"" << leaf->get_size() << "\">page_no=" << leaf->get_page_no() << "</TD></TR>\n";
            out << "<TR><TD COLSPAN=\"" << leaf->get_size() << "\">"
                << "max_size=" << leaf->get_max_size() << ",min_size=" << leaf->get_min_size() << "</TD></TR>\n";
            out << "<TR>";
            for (int i = 0; i < leaf->get_size(); i++) {
                out << "<TD>" << *reinterpret_cast<int*>(leaf->get_key(i)) << "</TD>\n";
            }
            out << "</TR>";
            // Print table end
            out << "</TABLE>>];\n";
            // Print Leaf node link if there is a next page
            if (leaf->get_next_leaf() != INVALID_PAGE_ID && leaf->get_next_leaf() > 1) {
                // 注意加上一个大于1的判断条件，否则若GetNextPageNo()是1，会把1那个结点也画出来
                out << leaf_prefix << leaf->get_page_no() << " -> " << leaf_prefix << leaf->get_next_leaf() << ";\n";
                out << "{rank=same " << leaf_prefix << leaf->get_page_no() << " " << leaf_prefix << leaf->get_next_leaf()
                    << "};\n";
            }

            // Print parent links if there is a parent
            if (leaf->get_parent_page_no() != INVALID_PAGE_ID) {
                out << internal_prefix << leaf->get_parent_page_no() << ":p" << leaf->get_page_no() << " -> " << leaf_prefix
                    << leaf->get_page_no() << ";\n";
            }
        } else {
            IxNodeHandle *inner = node;
            // Print node name
            out << internal_prefix << inner->get_page_no();
            // Print node properties
            out << "[shape=plain color=pink ";  // why not?
            // Print data of the node
            out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
            // Print data
            out << "<TR><TD COLSPAN=\"" << inner->get_size() << "\">page_no=" << inner->get_page_no() << "</TD></TR>\n";
            out << "<TR><TD COLSPAN=\"" << inner->get_size() << "\">"
                << "max_size=" << inner->get_max_size() << ",min_size=" << inner->get_min_size() << "</TD></TR>\n";
            out << "<TR>";
            for (int i = 0; i < inner->get_size(); i++) {
                out << "<TD PORT=\"p" << inner->value_at(i) << "\">";
                out << inner->key_at(i);
                // if (inner->KeyAt(i) != 0) {  // 原判断条件是if (i > 0)
                //     out << inner->KeyAt(i);
                // } else {
                //     out << " ";
                // }
                out << "</TD>\n";
            }
            out << "</TR>";
            // Print table end
            out << "</TABLE>>];\n";
            // Print Parent link
            if (inner->get_parent_page_no() != INVALID_PAGE_ID) {
                out << internal_prefix << inner->get_parent_page_no() << ":p" << inner->get_page_no() << " -> "
                    << internal_prefix << inner->get_page_no() << ";\n";
            }
            // Print leaves
            for (int i = 0; i < inner->get_size(); i++) {
                IxNodeHandle *child_node = ih->fetch_node(inner->value_at(i));
                ToGraph(ih, child_node, bpm, out);  // 继续递归
                if (i > 0) {
                    IxNodeHandle *sibling_node = ih->fetch_node(inner->value_at(i - 1));
                    if (!sibling_node->is_leaf_page() && !child_node->is_leaf_page()) {
                        out << "{rank=same " << internal_prefix << sibling_node->get_page_no() << " " << internal_prefix
                            << child_node->get_page_no() << "};\n";
                    }
                    bpm->unpin_page(sibling_node->get_page_id(), false);
                }
            }
        }
        bpm->unpin_page(node->get_page_id(), false);
    }

    /**
     * @brief 生成B+树可视化图
     *
     * @param bpm 缓冲池
     * @param outf dot文件名
     */
    void Draw(BufferPoolManager *bpm, const std::string &outf) {
        std::ofstream out(outf);
        out << "digraph G {" << std::endl;
        
        IxNodeHandle *node = ih_->fetch_node(ih_->file_hdr_->root_page_);
        ToGraph(ih_.get(), node, bpm, out);
        out << "}" << std::endl;
        out.close();

        // 由dot文件生成png文件
        std::string prefix = outf;
        prefix.replace(outf.rfind(".dot"), 4, "");
        std::string png_name = prefix + ".png";
        std::string cmd = "dot -Tpng " + outf + " -o " + png_name;
        system(cmd.c_str());

        // printf("Generate picture: build/%s/%s\n", TEST_DB_NAME.c_str(), png_name.c_str());
        printf("Generate picture: %s\n", png_name.c_str());
    }

    /**------ 以下为辅助检查函数 ------*/

    /**
     * @brief 检查叶子层的前驱指针和后继指针
     *
     * @param ih
     */
    void check_leaf(const IxIndexHandle *ih) {
        // check leaf list
        page_id_t leaf_no = ih->file_hdr_->first_leaf_;
        while (leaf_no != IX_LEAF_HEADER_PAGE) {
            IxNodeHandle *curr = ih->fetch_node(leaf_no);
            IxNodeHandle *prev = ih->fetch_node(curr->get_prev_leaf());
            IxNodeHandle *next = ih->fetch_node(curr->get_next_leaf());
            // Ensure prev->next == curr && next->prev == curr
            ASSERT_EQ(prev->get_next_leaf(), leaf_no);
            ASSERT_EQ(next->get_prev_leaf(), leaf_no);
            leaf_no = curr->get_next_leaf();
            buffer_pool_manager_->unpin_page(curr->get_page_id(), false);
            buffer_pool_manager_->unpin_page(prev->get_page_id(), false);
            buffer_pool_manager_->unpin_page(next->get_page_id(), false);
        }
    }

    /**
     * @brief dfs遍历整个树，检查孩子结点的第一个和最后一个key是否正确
     *
     * @param ih 树
     * @param now_page_no 当前遍历到的结点
     */
    void check_tree(const IxIndexHandle *ih, int now_page_no) {
        IxNodeHandle *node = ih->fetch_node(now_page_no);
        if (node->is_leaf_page()) {
            buffer_pool_manager_->unpin_page(node->get_page_id(), false);
            return;
        }
        for (int i = 0; i < node->get_size(); i++) {                 // 遍历node的所有孩子
            IxNodeHandle *child = ih->fetch_node(node->value_at(i));  // 第i个孩子
            // check parent
            assert(child->get_parent_page_no() == now_page_no);
            // check first key
            int node_key = node->key_at(i);  // node的第i个key
            int child_first_key = child->key_at(0);
            int child_last_key = child->key_at(child->get_size() - 1);
            if (i != 0) {
                // 除了第0个key之外，node的第i个key与其第i个孩子的第0个key的值相同
                ASSERT_EQ(node_key, child_first_key);
            }
            if (i + 1 < node->get_size()) {
                // 满足制约大小关系
                ASSERT_LT(child_last_key, node->key_at(i + 1));  // child_last_key < node->KeyAt(i + 1)
            }

            buffer_pool_manager_->unpin_page(child->get_page_id(), false);

            check_tree(ih, node->value_at(i));  // 递归子树
        }
        buffer_pool_manager_->unpin_page(node->get_page_id(), false);
    }

    /**
     * @brief
     *
     * @param ih
     * @param mock 函数外部记录插入/删除后的(key,rid)
     */
    void check_all(IxIndexHandle *ih, const std::multimap<int, Rid> &mock) {
        check_tree(ih, ih->file_hdr_->root_page_);
        if (!ih->is_empty()) {
            check_leaf(ih);
        }

        for (auto &entry : mock) {
            int mock_key = entry.first;
            // test lower bound
            {
                auto mock_lower = mock.lower_bound(mock_key);        // multimap的lower_bound方法
                Iid iid = ih->lower_bound((const char *)&mock_key, txn_.get());  // IxIndexHandle的lower_bound方法
                Rid rid = ih->get_rid(iid);
                ASSERT_EQ(rid, mock_lower->second);
            }
            // test upper bound
            {
                auto mock_upper = mock.upper_bound(mock_key);
                Iid iid = ih->upper_bound((const char *)&mock_key, txn_.get());
                if (iid != ih->leaf_end()) {
                    Rid rid = ih->get_rid(iid);
                    ASSERT_EQ(rid, mock_upper->second);
                }
            }
        }

        // test scan
        IxScan scan(ih, ih->leaf_begin(), ih->leaf_end(), buffer_pool_manager_.get());
        auto it = mock.begin();
        int leaf_no = ih->file_hdr_->first_leaf_;
        assert(leaf_no == scan.iid().page_no);
        // 注意在scan里面是iid的slot_no进行自增
        while (!scan.is_end() && it != mock.end()) {
            Rid mock_rid = it->second;
            Rid rid = scan.rid();
            ASSERT_EQ(rid, mock_rid);
            // go to next slot_no
            it++;
            scan.next();
        }
        ASSERT_EQ(scan.is_end(), true);
        ASSERT_EQ(it, mock.end());
    }

};

/**
 * @brief insert 1~10 and delete 1~9 (will draw pictures)
 * 
 * @note lab2 计分：10 points
 */
TEST_F(BPlusTreeTests, InsertAndDeleteTest1) {
    const int64_t scale = 10;
    const int64_t delete_scale = 9;  // 删除的个数最好小于scale，等于的话会变成空树
    const int order = 4;

    assert(order > 2 && order <= ih_->file_hdr_->btree_order_);
    ih_->file_hdr_->btree_order_ = order;

    std::vector<int64_t> keys;
    for (int64_t key = 1; key <= scale; key++) {
        keys.push_back(key);
    }

    // insert keys
    const char *index_key;
    for (auto key : keys) {
        int32_t value = key & 0xFFFFFFFF;  // key的低32位
        Rid rid = {.page_no = static_cast<int32_t>(key >> 32),
                   .slot_no = value};  // page_id = (key>>32), slot_num = (key & 0xFFFFFFFF)
        index_key = (const char *)&key;
        bool insert_ret = ih_->insert_entry(index_key, rid, txn_.get());  // 调用Insert
        ASSERT_EQ(insert_ret, true);
    }
    Draw(buffer_pool_manager_.get(), "insert10.dot");

    // scan keys by GetValue()
    std::vector<Rid> rids;
    for (auto key : keys) {
        rids.clear();
        index_key = (const char *)&key;
        ih_->get_value(index_key, &rids, txn_.get());  // 调用GetValue
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].slot_no, value);
    }

    // delete keys
    std::vector<int64_t> delete_keys;
    for (int64_t key = 1; key <= delete_scale; key++) {  // 1~9
        delete_keys.push_back(key);
    }
    for (auto key : delete_keys) {
        index_key = (const char *)&key;
        bool delete_ret = ih_->delete_entry(index_key, txn_.get());  // 调用Delete
        ASSERT_EQ(delete_ret, true);

//        Draw(buffer_pool_manager_.get(), "InsertAndDeleteTest1_delete" + std::to_string(key) + ".dot");
    }

    // scan keys by Ixscan
    int64_t start_key = *delete_keys.rbegin() + 1;
    int64_t current_key = start_key;
    int64_t size = 0;

    IxScan scan(ih_.get(), ih_->leaf_begin(), ih_->leaf_end(), buffer_pool_manager_.get());
    while (!scan.is_end()) {
        auto rid = scan.rid();
        EXPECT_EQ(rid.page_no, 0);
        EXPECT_EQ(rid.slot_no, current_key);
        current_key++;
        size++;
        scan.next();
    }
    EXPECT_EQ(size, keys.size() - delete_keys.size());
}

/**
 * @brief insert 1~10 and delete 1,2,3,4,7,5 (will draw pictures)
 *
 * @note lab2 计分：10 points
 */
TEST_F(BPlusTreeTests, InsertAndDeleteTest2) {
    const int64_t scale = 10;
    const int order = 4;

    assert(order > 2 && order <= ih_->file_hdr_->btree_order_);
    ih_->file_hdr_->btree_order_ = order;

    std::vector<int64_t> keys;
    for (int64_t key = 1; key <= scale; key++) {
        keys.push_back(key);
    }

    // insert keys
    const char *index_key;
    for (auto key : keys) {
        int32_t value = key & 0xFFFFFFFF;  // key的低32位
        Rid rid = {.page_no = static_cast<int32_t>(key >> 32),
                   .slot_no = value};  // page_id = (key>>32), slot_num = (key & 0xFFFFFFFF)
        index_key = (const char *)&key;
        bool insert_ret = ih_->insert_entry(index_key, rid, txn_.get());  // 调用Insert
        ASSERT_EQ(insert_ret, true);
    }
    // Draw(buffer_pool_manager_.get(), "insert10.dot");

    // scan keys by GetValue()
    std::vector<Rid> rids;
    for (auto key : keys) {
        rids.clear();
        index_key = (const char *)&key;
        ih_->get_value(index_key, &rids, txn_.get());  // 调用GetValue
        EXPECT_EQ(rids.size(), 1);

        int64_t value = key & 0xFFFFFFFF;
        EXPECT_EQ(rids[0].slot_no, value);
    }

    // delete keys
    std::vector<int64_t> delete_keys = {1, 2, 3, 4, 7, 5};
    for (auto key : delete_keys) {
        index_key = (const char *)&key;
        bool delete_ret = ih_->delete_entry(index_key, txn_.get());  // 调用Delete
        ASSERT_EQ(delete_ret, true);

        // Draw(buffer_pool_manager_.get(), "InsertAndDeleteTest2_delete" + std::to_string(key) + ".dot");
    }
}

/**
 * @brief 随机插入和删除多个键值对
 * 
 * @note lab2 计分：20 points
 */
TEST_F(BPlusTreeTests, LargeScaleTest) {
    const int order = 255;  // 若order太小，而插入数据过多，将会超出缓冲池
    const int scale = 20000;

    if (order >= 2 && order <= ih_->file_hdr_->btree_order_) {
        ih_->file_hdr_->btree_order_ = order;
    }
    int add_cnt = 0;
    int del_cnt = 0;
    std::multimap<int, Rid> mock;
    mock.clear();
    int num = 0;
    while (add_cnt + del_cnt < scale) {
        double dice = rand() * 1. / RAND_MAX;
        double insert_prob = 1. - mock.size() / (0.5 * scale);
        if (mock.empty() || dice < insert_prob) {
            // Insert
            int rand_key = rand() % scale;
            if (mock.find(rand_key) != mock.end()) {  // 防止插入重复的key
                // printf("重复key=%d!\n", rand_key);
                continue;
            }
            Rid rand_val = {.page_no = rand(), .slot_no = rand()};
            printf("insert rand key=%d\n", rand_key);
            bool insert_ret = ih_->insert_entry((const char *)&rand_key, rand_val, txn_.get());  // 调用Insert
            ASSERT_EQ(insert_ret, true);
            mock.insert(std::make_pair(rand_key, rand_val));
            add_cnt++;
            // Draw(buffer_pool_manager_.get(),
            //      "MixTest2_" + std::to_string(num) + "_insert" + std::to_string(rand_key) + ".dot");
        } else {
            // Delete
            if (mock.size() == 1) {  // 只剩最后一个结点时不删除，以防变成空树
                continue;
            }
            int rand_idx = rand() % mock.size();
            auto it = mock.begin();
            for (int k = 0; k < rand_idx; k++) {
                it++;
            }
            int key = it->first;
            printf("delete rand key=%d\n", key);
            if(key == 129){
                std::cout << "now" ;
            }
            bool delete_ret = ih_->delete_entry((const char *)&key, txn_.get());
            ASSERT_EQ(delete_ret, true);
            mock.erase(it);
            del_cnt++;
            // Draw(buffer_pool_manager_.get(),
            //      "MixTest2_" + std::to_string(num) + "_delete" + std::to_string(key) + ".dot");
        }
        // check_all(ih_.get(), mock);
        num++;
    }
    std::cout << "Insert keys count: " << add_cnt << '\n' << "Delete keys count: " << del_cnt << '\n';
    check_all(ih_.get(), mock);
}