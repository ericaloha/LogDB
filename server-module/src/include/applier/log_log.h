#ifndef NFS_GANESHA_SRC_INCLUDE_APPLIER_LOG_LOG_H_
#define NFS_GANESHA_SRC_INCLUDE_APPLIER_LOG_LOG_H_
#include <vector>
#include <cstddef>
#include <string>
#include <pthread.h>
#include <memory>
#include <list>
#include <unordered_map>
#include <atomic>
#include <unordered_set>
#include "applier/applier_config.h"
#include "applier/bean.h"
#include "applier/hash_util.h"

#define START_THREAD(name, thread_id, thread_routine, routine_args)                                              \
do {                                                                                                             \
    int rc = 0;                                                                                                  \
    pthread_attr_t thread_attr;                                                                                  \
    if (pthread_attr_init(&thread_attr) != 0)                                                                    \
        LogDebug(COMPONENT_THREAD, "can't init pthread's attributes");                                           \
    if (pthread_attr_setscope(&thread_attr, PTHREAD_SCOPE_SYSTEM) != 0)                                          \
        LogDebug(COMPONENT_THREAD, "can't set pthread's scope");                                                 \
    if (pthread_attr_setdetachstate(&thread_attr, PTHREAD_CREATE_JOINABLE) != 0)                                 \
        LogDebug(COMPONENT_THREAD, "can't set pthread's join state");                                            \
    rc = pthread_create(thread_id, &thread_attr, thread_routine, routine_args);                                  \
    if (rc != 0) {                                                                                               \
        LogFatal(COMPONENT_THREAD, "Could not create %s thread, error = %d (%s)", name, errno, strerror(errno)); \
    }                                                                                                            \
    LogEvent(COMPONENT_THREAD, "%s thread was started successfully", name);                                      \
    pthread_attr_destroy(&thread_attr);                                                                          \
} while (0)

class PageAddress {
public:
    PageAddress() = default;
    PageAddress(space_id_t space_id, page_id_t page_id) noexcept : space_id(space_id), page_id(page_id) {}
    [[nodiscard]] space_id_t SpaceId() const noexcept {return space_id;}
    [[nodiscard]] page_id_t PageId() const noexcept {return page_id;}
    bool operator==(const PageAddress &other) const noexcept {
        return space_id == other.space_id && page_id == other.page_id;
    }
    bool operator!=(const PageAddress &other) const noexcept {
        return !(operator==(other));
    }

private:
    space_id_t space_id {};
    page_id_t page_id {};
};

namespace std {

/** Implements std::hash on PageAddress */
    template <>
    struct hash<PageAddress> {
        auto operator()(const PageAddress &page_address) const -> std::size_t {
            auto space_id = page_address.SpaceId();
            auto page_id = page_address.PageId();
            return HashUtil::CombineHashes(HashUtil::Hash(&space_id), HashUtil::Hash(&page_id));
        }
    };

}  // namespace std

struct log_parser_t {
    pthread_t thread_id {0};
    size_t round {0};

    unsigned char *parse_buf {nullptr}; // 要么指向parse_buf_back，要么指向log_group的log_buf

    // 如果当前需要解析的一批log，在log buf中出现首尾相连的情况，
    // 为了保证parse buf是连续的，就必须把首尾两端log拷贝到parse_buf_back中
    std::unique_ptr<unsigned char[]> parse_buf_back {std::unique_ptr<unsigned char[]>(new unsigned char [PER_LOG_FILE_SIZE * LOG_FILE_NUMBER])};

    int log_dispatch_number_table[APPLIER_THREAD] {}; // 记录当前已经分配给每一个log applier的log数量

    std::unordered_map<PageAddress, int> log_dispatch_trace_table {}; // 记录某一个space id, page id被分配到了哪一个log applier

    size_t parsed_lsn;
};

extern log_parser_t log_parser;

using apply_task_bucket = std::unordered_map<PageAddress, std::list<LogEntry>>;

struct log_applier_t {
public:
    log_applier_t();
    // logs need to be applied
    std::vector<PageAddress> logs {};

    pthread_t thread_id {0};

    std::atomic_bool is_running {false}; // 当前线程是否空闲

    std::atomic_bool need_process {false}; // 当前线程是不是有任务等待被处理

    pthread_mutex_t mutex {}; // 保护自己

    pthread_cond_t need_process_cond {}; // log apply scheduler通知log apply worker起来干活
};

struct fsal_obj_handle;

struct DataPageGroup {
public:
    static DataPageGroup &Get();
    void Insert(const std::string &filename, space_id_t space_id);
    void Insert(const struct fsal_obj_handle* handle, space_id_t space_id);
    int Exist(const std::string &filename);
    int Exist(const struct fsal_obj_handle* handle);
    bool Exist(space_id_t space_id);
private:
    DataPageGroup() { pthread_rwlock_init(&rw_lock_, nullptr); }
    ~DataPageGroup() { pthread_rwlock_destroy(&rw_lock_); }
    std::unordered_map<std::string, space_id_t> filename2space_id_ {};
    std::unordered_map<const struct fsal_obj_handle*, space_id_t> handle2space_id_ {};
    std::unordered_set<space_id_t> space_id_ {};
    pthread_rwlock_t rw_lock_ {};
};

extern std::vector<log_applier_t> log_appliers;

struct apply_task {
    apply_task() {
        for (int i = 0; i < APPLIER_THREAD; ++i) {
            log_hash.emplace_back(std::make_unique<apply_task_bucket>());
        }
    }
    std::size_t content_len {};
    std::vector<std::unique_ptr<apply_task_bucket>> log_hash {};
};

class PthreadMutexGuard {
public:
    explicit PthreadMutexGuard(pthread_mutex_t &lock) : lock_(lock) { pthread_mutex_lock(&lock_); }
    ~PthreadMutexGuard() { pthread_mutex_unlock(&lock_);}
private:
    pthread_mutex_t &lock_;
};

class ApplyIndex {
public:
    class IndexSegment {
    public:
        using log_list = std::unique_ptr<std::list<LogEntry>>;
    public:
        IndexSegment() = default;
        ~IndexSegment() = default;
        bool Insert(LogEntry &&log) {
            // 已经满了
            if (total_log_len_ >= log_len_limit_) {
                return false;
            }
            auto log_len = log.log_len_;
            PageAddress page_address(log.space_id_, log.page_id_);
            if (index_segment_.find(page_address) == index_segment_.end()) {
                auto [iter, success] = index_segment_.insert(std::make_pair(page_address, std::make_unique<std::list<LogEntry>>()));
                assert(success);
                iter->second->push_back(std::move(log));
            } else {
                index_segment_[page_address]->push_back(std::move(log));
            }
            total_log_len_ += log_len;
            return true;
        }
        bool Full() const {
            return total_log_len_ >= log_len_limit_;
        }
        log_list ExtractLog(const PageAddress &page_address) {
            auto iter = index_segment_.find(page_address);
            if (iter == index_segment_.end()) {
                return {nullptr};
            }
            log_list res = std::move(iter->second);
            index_segment_.erase(iter);
            return res;
        }
        bool Empty() const {return index_segment_.empty();}
        std::vector<PageAddress> Hint(size_t *log_len) {
            std::vector<PageAddress> res;
            for (const auto &item: index_segment_) {
                res.push_back(item.first);
            }
            if (log_len != nullptr) {
                *log_len = total_log_len_;
            }
            return res;
        }
    private:
        size_t total_log_len_ {0};
        size_t log_len_limit_ {APPLY_BATCH_SIZE}; // 一个index segment最多存储这么长的log
        std::unordered_map<PageAddress, log_list> index_segment_ {};
    };
public:
    ApplyIndex() {
        pthread_mutex_init(&lock_, nullptr);
        pthread_cond_init(&index_not_empty_cond_, nullptr);
        pthread_cond_init(&front_full_cond_, nullptr);
    }
    ~ApplyIndex() {
        pthread_mutex_destroy(&lock_);
        pthread_cond_destroy(&index_not_empty_cond_);
        pthread_cond_destroy(&front_full_cond_);
    }
    void InsertBack(LogEntry &&log) {
        PthreadMutexGuard guard(lock_);
        if (index_.empty()) {
            index_.push_back(std::make_unique<IndexSegment>());
            pthread_cond_signal(&index_not_empty_cond_);
        }
        if (index_.back()->Full()) {
            index_.push_back(std::make_unique<IndexSegment>());
        }
        index_.back()->Insert(std::move(log));
        if (index_.back()->Full()) {
            // 唤醒log applier scheduler
            pthread_cond_signal(&front_full_cond_);
        }
    }

    IndexSegment::log_list ExtractFront(const PageAddress &page_address) {

        PthreadMutexGuard guard(lock_);
        // wait until index_ is not empty
        while (index_.empty()) {
            pthread_cond_wait(&index_not_empty_cond_, &lock_);
        }

        auto res = index_.front()->ExtractLog(page_address);
        DeleteFrontSegment();

        return res;
    }

    std::vector<PageAddress> ExtractFrontHint(size_t *log_len) {

        PthreadMutexGuard guard(lock_);
        // wait until index_ is not empty
        while (index_.empty()) {
            pthread_cond_wait(&index_not_empty_cond_, &lock_);
        }

        // wait until index_.front is full
        while (!index_.front()->Full()) {
            pthread_cond_wait(&front_full_cond_, &lock_);
        }

        return index_.front()->Hint(log_len);

    }

    void DeleteFrontSegment() {
        if (index_.empty()) {
            return;
        }

        if (index_.front()->Empty()) {
            index_.erase(index_.begin());
        }
    }

    std::vector<IndexSegment::log_list> Search(const PageAddress &page_address) {
        PthreadMutexGuard guard(lock_);
        // wait until index_ is not empty
        std::vector<IndexSegment::log_list> res;
        for (auto &item: index_) {
            if (auto logs = item->ExtractLog(page_address); logs != nullptr) {
                res.push_back(std::move(logs));
            }
        }
        return res;
    }
private:
    pthread_cond_t index_not_empty_cond_ {};
    pthread_cond_t front_full_cond_ {};
    pthread_mutex_t lock_ {}; // protect index_
    std::list<std::unique_ptr<IndexSegment>> index_ {};
};

//extern pthread_mutex_t log_apply_task_mutex;
extern pthread_cond_t log_apply_condition; // 每次log parser 解析之后产生apply task，就会产生这个条件变量来唤醒log applier
//extern std::list<std::unique_ptr<apply_task>> apply_task_requests;
extern ApplyIndex apply_index;
extern pthread_mutex_t log_group_mutex;
extern pthread_cond_t log_parse_condition; // 每次log writer 写入，导致产生足够多的log，就会产生这个条件变量来唤醒log parser
extern pthread_cond_t log_write_condition; // 每次log applier 完成，释放出空间，就会产生这个条件变量来唤醒log writer
struct log_group_t {
    int log_file_number;
    size_t per_file_size; // in bytes
    std::vector<std::string> log_filenames;
    std::vector<std::string> log_file_full_paths;
    std::vector<struct fsal_obj_handle*> file_handles; // used by log writer

    size_t batch_size; // log parser线程每次读取batch_size * apply_thread_number大小的log，依次交给每一个apply线程进行处理

    size_t checkpoint_no;
    size_t checkpoint_lsn;

    std::atomic<size_t> parsed_isn;
    std::atomic<size_t> written_isn;
    std::atomic<size_t> applied_isn;

    // 在log group中的偏移量
    size_t checkpoint_offset;
    size_t start_offset; // mysql启动后，应该start_offset偏移量处开始按block写log
//    std::atomic<size_t> parsed_lsn;
    std::atomic<size_t> written_lsn;
    size_t applied_lsn;

    // offset in log group
    std::atomic<size_t> parsed_offset; // 下一次从这里开始解析
    size_t written_offset;
    size_t applied_offset;

    // how many bytes left in log buf that log writer can write
    size_t written_capacity;

    // how many bytes of redo log left, that parse thread need to parse
    // note!! written_lsn should always larger than parsed_lsn
    std::atomic<size_t> need_to_parse;

    // The number of bytes that have been applied but not yet parsed.
//    std::atomic<size_t> need_to_apply;

    unsigned char *log_meta_buf;
    uint32_t log_meta_buf_size;

    unsigned char *log_buf; // 不包括log 的元数据块

    uint32_t log_buf_size;

    uint32_t log_buf_size_per_file; // 每一个log file需要多大的log buf，不包括log 的元数据块还有log block内的header和trail

    bool first_written;
};

extern log_group_t log_group;

/**
 * find the max checkpoint no and its correspond lsn and its offset in log group
 * @param checkpoint_lsn
 * @param checkpoint_no
 * @param checkpoint_offset
 */
void find_max_checkpoint(const unsigned char *log_meta_buf, size_t *checkpoint_lsn, size_t *checkpoint_no, size_t *checkpoint_offset);

size_t log_group_off_to_log_buf_off(size_t log_group_off);
#endif
