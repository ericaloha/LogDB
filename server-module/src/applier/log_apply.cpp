#include <memory>
#include "applier/log_apply.h"
#include "applier/log_log.h"
#include "applier/utility.h"
#include "applier/buffer_pool.h"
#include "applier/log_parse.h"
#include "applier/interface.h"

#ifdef __cplusplus
extern "C" {
#endif
#include "common_utils.h"
#include "log.h"
#ifdef __cplusplus
}
#endif

// 检查是不是所有的log_applier都是空闲的
static bool log_apply_all_idle() {
    return std::all_of(log_appliers.cbegin(), log_appliers.cend(), [](const auto &log_applier) -> bool {
        return log_applier.is_running == false;
    });
}

// 从index上摘下task请求，并且等待所有log worker变成空闲状态
static std::vector<PageAddress> log_apply_scheduler_acquire(size_t *total_log_len) {
//    PTHREAD_MUTEX_lock(&log_apply_task_mutex);
//    while (apply_task_requests.empty()) {
//        pthread_cond_wait(&log_apply_condition, &log_apply_task_mutex);
//    }
//    auto task = std::move(apply_task_requests.front());
//    apply_task_requests.erase(apply_task_requests.begin());
//    PTHREAD_MUTEX_unlock(&log_apply_task_mutex);

    // 自旋等待所有log apply worker变成空闲状态
    while (!log_apply_all_idle());

    return apply_index.ExtractFrontHint(total_log_len);
}

static bool log_apply_apply_one_log(Page *page, const LogEntry &log) {
    byte *ret;
    switch (log.type_) {
        case MLOG_1BYTE:
        case MLOG_2BYTES:
        case MLOG_4BYTES:
        case MLOG_8BYTES:
            ret = ParseOrApplyNBytes(log.type_, log.log_body_start_ptr_, log.log_body_end_ptr_, page->GetData());
            assert(ret != nullptr);
            return ret != nullptr;
        case MLOG_WRITE_STRING:
            ret = ParseOrApplyString(log.log_body_start_ptr_, log.log_body_end_ptr_, page->GetData());
            assert(ret != nullptr);
            return ret != nullptr;
        case MLOG_COMP_PAGE_CREATE:
            ret = ApplyCompPageCreate(page->GetData());
            return ret != nullptr;
        case MLOG_INIT_FILE_PAGE2:
            return ApplyInitFilePage2(log, page);
        case MLOG_COMP_REC_INSERT:
            return ApplyCompRecInsert(log, page);
        case MLOG_COMP_REC_CLUST_DELETE_MARK:
            return ApplyCompRecClusterDeleteMark(log, page);
        case MLOG_REC_SEC_DELETE_MARK:
            return ApplyRecSecondDeleteMark(log, page);
        case MLOG_COMP_REC_SEC_DELETE_MARK:
            return ApplyCompRecSecondDeleteMark(log, page);
        case MLOG_COMP_REC_UPDATE_IN_PLACE:
            return ApplyCompRecUpdateInPlace(log, page);
        case MLOG_COMP_REC_DELETE:
            return ApplyCompRecDelete(log, page);
        case MLOG_COMP_LIST_END_COPY_CREATED:
            return ApplyCompListEndCopyCreated(log, page);
        case MLOG_COMP_PAGE_REORGANIZE:
            return ApplyCompPageReorganize(log, page);
        case MLOG_COMP_LIST_START_DELETE:
        case MLOG_COMP_LIST_END_DELETE:
            return ApplyCompListDelete(log, page);
        case MLOG_IBUF_BITMAP_INIT:
            return ApplyIBufBitmapInit(log, page);
        default:
            return false;
    }
}

void log_apply_do_apply(const PageAddress &page_address, std::list<LogEntry> *log_entry_list) {
    auto space_id = page_address.SpaceId();

    // skip!
    if (!(DataPageGroup::Get().Exist(space_id))) {
        return;
    }

    auto page_id = page_address.PageId();
    // 获取需要的page
    Page *page = buffer_pool.GetPage(space_id, page_id);

    // 磁盘上没有，需要新create一个page
    if (page == nullptr) {
        page = buffer_pool.NewPage(space_id, page_id);
    }

    lsn_t page_lsn = page->GetLSN();
    for (const auto &log: (*log_entry_list)) {
        lsn_t log_lsn = log.log_start_lsn_;
//        std::cout << "space id = " << space_id << ", page id = " << page_id << ", log type = " << GetLogString(log.type_);
        // skip!
        if (page_lsn > log_lsn) {
//            std::cout << "skip" << std::endl;
            continue;
        } else {
//            std::cout << std::endl;
        }

        if (log_apply_apply_one_log(page, log)) {
            page->WritePageLSN(log_lsn + log.log_len_);
            page->WriteCheckSum(BUF_NO_CHECKSUM_MAGIC);
        }
    }
    buffer_pool.WriteBackLock(space_id, page_id);
    BufferPool::ReleasePage(page);
}

static void log_apply_worker_work(int worker_index) {
    PTHREAD_MUTEX_lock(&(log_appliers[worker_index].mutex));
    while (!(log_appliers[worker_index].need_process)) {
        pthread_cond_wait(&(log_appliers[worker_index].need_process_cond), &(log_appliers[worker_index].mutex));
    }

    log_appliers[worker_index].is_running = true;

    // do apply
    for (const auto &page_address: log_appliers[worker_index].logs) {
        auto log_entry_list = apply_index.ExtractFront(page_address);
        if (log_entry_list == nullptr) {
            // 这条log可能已经被其它的data page reader线程抽走了
            continue;
        }
        log_apply_do_apply(page_address, log_entry_list.get());
    }


    // 放掉lock
    log_appliers[worker_index].logs.clear();
    log_appliers[worker_index].need_process = false;
    log_appliers[worker_index].is_running = false;
    PTHREAD_MUTEX_unlock(&(log_appliers[worker_index].mutex));
}

static void *log_apply_worker_routine(void *worker_index) {
    int *index_ptr = static_cast<int *>(worker_index);
    int index = *(index_ptr);
    delete index_ptr;
    for (;;) {
        log_apply_worker_work(index);
    }
}

static void *log_apply_scheduler_routine(void *scheduler_index) {
    int *index_ptr = static_cast<int *>(scheduler_index);
    int index = *(index_ptr);
    delete index_ptr;
    for (;;) {

        size_t total_log_len = 0;
        auto task = log_apply_scheduler_acquire(&total_log_len);

        LogEvent(COMPONENT_FSAL, "log applier starting apply %zu bytes log", total_log_len);
        auto need_to_apply = total_log_len;

        // 把每一个task分配给相应的worker
        size_t next_applier = 0;
        for (const auto &item: task) {
            log_appliers[next_applier].logs.push_back(item);
            next_applier = (next_applier + 1) % log_appliers.size();
        }

        // 唤醒相应的worker
        for (auto & log_applier : log_appliers) {

            PTHREAD_MUTEX_lock(&(log_applier.mutex));
            log_applier.need_process = true;
            pthread_cond_signal(&(log_applier.need_process_cond));
            PTHREAD_MUTEX_unlock(&(log_applier.mutex));

        }

        // 自己变成worker进行工作
        log_apply_worker_work(index);

        LogEvent(COMPONENT_FSAL, "applied %zu bytes log", need_to_apply);
        // 自旋等待所有log worker变成空闲状态
        while (!log_apply_all_idle());

        PTHREAD_MUTEX_lock(&(log_group_mutex));

        log_group.applied_isn += need_to_apply;
        log_group.written_capacity += need_to_apply;

        // 唤醒log writer
        pthread_cond_signal(&log_write_condition);

        PTHREAD_MUTEX_unlock(&(log_group_mutex));
    }
}

void log_apply_thread_start(int n_thread) {
    assert(APPLIER_THREAD >= 1); // 最少要有一个log apply线程
    assert(log_appliers.size() == APPLIER_THREAD);

    // 首先启动scheduler
    int *scheduler_index = new int(0);
    START_THREAD("log apply scheduler", &log_appliers[0].thread_id, log_apply_scheduler_routine, (void *)(scheduler_index));
    n_thread -= 1;

    // 启动剩下的apply worker
    for (int i = 0; i < n_thread; ++i) {
        std::string thread_name = "log apply scheduler";
        thread_name += std::to_string(i);
        int *work_index = new int(i);
        START_THREAD(thread_name.c_str(), &log_appliers[i].thread_id, log_apply_worker_routine, (void *)(work_index));
        n_thread--;
    }
}