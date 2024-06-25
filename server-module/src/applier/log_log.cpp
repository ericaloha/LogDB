#include <unistd.h>
#include <cassert>
#include "applier/log_log.h"
#include "applier/applier_config.h"
#include "applier/utility.h"
#ifdef __cplusplus
extern "C" {
#endif
#include "common_utils.h"

#ifdef __cplusplus
}
#endif
log_group_t log_group;

//pthread_mutex_t log_apply_task_mutex;
pthread_cond_t log_apply_condition; // 每次log parser 解析之后产生apply task，就会产生这个条件变量来唤醒log applier
//std::list<std::unique_ptr<apply_task>> apply_task_requests;
ApplyIndex apply_index;
pthread_mutex_t log_group_mutex;
pthread_cond_t log_parse_condition; // 每次log writer 写入，导致产生足够多的log，就会产生这个条件变量来唤醒log parser
pthread_cond_t log_write_condition;
log_parser_t log_parser {};
std::vector<log_applier_t> log_appliers {APPLIER_THREAD};

void find_max_checkpoint(const unsigned char *log_meta_buf, size_t *checkpoint_lsn, size_t *checkpoint_no, size_t *checkpoint_offset) {

    size_t checkpoint_lsn1 = mach_read_from_8(log_meta_buf + LOG_CHECKPOINT_1 + LOG_CHECKPOINT_LSN);
    size_t lsn_offset1 = mach_read_from_8(log_meta_buf + LOG_CHECKPOINT_1 + LOG_CHECKPOINT_OFFSET);
    size_t checkpoint_no1 = mach_read_from_8(log_meta_buf + LOG_CHECKPOINT_1 + LOG_CHECKPOINT_NO);

    size_t checkpoint_lsn3 = mach_read_from_8(log_meta_buf + LOG_CHECKPOINT_2 + LOG_CHECKPOINT_LSN);
    size_t lsn_offset3 = mach_read_from_8(log_meta_buf + LOG_CHECKPOINT_2 + LOG_CHECKPOINT_OFFSET);
    size_t checkpoint_no3 = mach_read_from_8(log_meta_buf + LOG_CHECKPOINT_2 + LOG_CHECKPOINT_NO);

    if (checkpoint_no1 > checkpoint_no3) {
        *checkpoint_no = checkpoint_no1;
        *checkpoint_lsn = checkpoint_lsn1;
        *checkpoint_offset = lsn_offset1;
        assert(checkpoint_lsn1 >= checkpoint_lsn3);
        assert(lsn_offset1 >= lsn_offset3);
    } else {
        *checkpoint_no = checkpoint_no3;
        *checkpoint_lsn = checkpoint_lsn3;
        *checkpoint_offset = lsn_offset3;
        assert(checkpoint_lsn1 <= checkpoint_lsn3);
        assert(lsn_offset1 <= lsn_offset3);
    }
}

size_t log_group_off_to_log_buf_off(size_t log_group_off) {
    auto n_file = log_group_off / log_group.per_file_size; // 这是第几个文件
    auto off_in_file = log_group_off % log_group.per_file_size; // 在文件内的偏移量

    auto n_block = off_in_file / LOG_BLOCK_SIZE; // 位于当前file的第几个block
    auto off_in_block = off_in_file % LOG_BLOCK_SIZE; // 在block内的偏移量

    assert(!(0 <= n_block && n_block < N_LOG_METADATA_BLOCKS)); // 不能落在log file的前四个meta data block上
    assert(!(0 <= off_in_block && off_in_block < LOG_BLOCK_HDR_SIZE)); // 不能落在block内的hdr上
    assert(!(LOG_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE <= off_in_block && off_in_block < LOG_BLOCK_SIZE)); // 不能落在block内的trl上

    size_t log_buf_off = 0;
    log_buf_off += log_group.log_buf_size_per_file * n_file;
    log_buf_off += (n_block - N_LOG_METADATA_BLOCKS) * (LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE);
    log_buf_off += off_in_block - LOG_BLOCK_HDR_SIZE;

    return log_buf_off;
}

log_applier_t::log_applier_t() {
    PTHREAD_MUTEX_init(&mutex, NULL);
    PTHREAD_COND_init(&need_process_cond, NULL);
}

void DataPageGroup::Insert(const std::string &filename, space_id_t space_id) {
    pthread_rwlock_wrlock(&rw_lock_);
    filename2space_id_[filename] = space_id;
    space_id_.insert(space_id);
    pthread_rwlock_unlock(&rw_lock_);
}

DataPageGroup &DataPageGroup::Get() {
    static DataPageGroup data_page_group;
    return data_page_group;
}

void DataPageGroup::Insert(const struct fsal_obj_handle *handle, space_id_t space_id) {
    pthread_rwlock_wrlock(&rw_lock_);
    handle2space_id_[handle] = space_id;
    space_id_.insert(space_id);
    pthread_rwlock_unlock(&rw_lock_);
}

int DataPageGroup::Exist(const std::string &filename) {
    int res = -1;
    pthread_rwlock_rdlock(&rw_lock_);
    if (auto iter = filename2space_id_.find(filename); iter != filename2space_id_.end()) {
        res = static_cast<int>(iter->second);
    }
    pthread_rwlock_unlock(&rw_lock_);
    return res;
}

int DataPageGroup::Exist(const struct fsal_obj_handle *handle) {
    int res = -1;
    pthread_rwlock_rdlock(&rw_lock_);
    if (auto iter = handle2space_id_.find(handle); iter != handle2space_id_.end()) {
        res = static_cast<int>(iter->second);
    }
    pthread_rwlock_unlock(&rw_lock_);
    return res;
}

bool DataPageGroup::Exist(space_id_t space_id) {
    return space_id_.find(space_id) != space_id_.end();
}
