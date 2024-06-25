#ifndef APPLIER_LOG_PARSE_H_
#define APPLIER_LOG_PARSE_H_
#include <pthread.h>
#include <memory>
#include "applier/bean.h"
#include "applier/log_type.h"
#include "applier/applier_config.h"
#include "applier/buffer_pool.h"
#include "applier/log_log.h"


void log_parse_thread_start(void );


/** Tries to parse a single log record.
@param[out]	type		log record type
@param[in]	ptr		pointer to a buffer
@param[in]	end_ptr		end of the buffer
@param[out]	space_id	tablespace identifier
@param[out]	page_no		page number
@param[out]	body		start of log record body
@return length of the record, or 0 if the record was not complete */
uint32_t
ParseSingleLogRecord(
    LOG_TYPE &type,
    const byte* ptr,
    const byte* end_ptr,
    space_id_t &space_id,
    page_id_t &page_id,
    byte** body);

/**
 * Parse or apply MLOG_1BYTE、MLOG_2BYTES、MLOG_4BYTES、MLOG_8BYTES.
 * This function just parse the log if the page is nullptr.
 * 如果传入的page参数不为nullptr，才会启动apply
 * @param type Which type this log belongs to.
 * @param log_body_start_ptr
 * @param log_body_end_ptr
 * @param page
 * @return
 */
byte* ParseOrApplyNBytes(LOG_TYPE type, const byte* log_body_start_ptr, const byte*	log_body_end_ptr, byte *page);

/**
 * Parse or apply MLOG_WRITE_STRING log.
 * @param log_body_start_ptr 指向log body的起始指针，闭区间
 * @param log_body_end_ptr 指向log body的结束指针，开区间
 * @param page 要恢复的page
 * @return
 */
byte* ParseOrApplyString(byte* log_body_start_ptr, const byte* log_body_end_ptr, byte* page);

/**
 * Apply MLOG_COMP_PAGE_CREATE log.
 * @param page The page you want to apply.
 */
byte* ApplyCompPageCreate(byte* page);

/**
 * Apply MLOG_INIT_FILE_PAGE2 log.
 * @param log
 * @param page
 */
bool ApplyInitFilePage2(const LogEntry &log, Page *page);

/**
 * Apply MLOG_COMP_REC_INSERT log.
 * @param log
 * @param page
 */
bool ApplyCompRecInsert(const LogEntry &log, Page *page);

/**
 * Apply MLOG_COMP_REC_CLUST_DELETE_MARK
 * @param log
 * @param page
 */
bool ApplyCompRecClusterDeleteMark(const LogEntry &log, Page *page);


bool ApplyRecSecondDeleteMark(const LogEntry &log, Page *page);


/**
 * Apply MLOG_COMP_REC_UPDATE_IN_PLACE
 * @param log
 * @param page
 */
bool ApplyCompRecUpdateInPlace(const LogEntry &log, Page *page);


bool ApplyCompRecSecondDeleteMark(const LogEntry &log, Page *page);

bool ApplyCompRecDelete(const LogEntry &log, Page *page);

bool ApplyCompListEndCopyCreated(const LogEntry &log, Page *page);

bool ApplyCompPageReorganize(const LogEntry &log, Page *page);

bool ApplyCompListDelete(const LogEntry &log, Page *page);

bool ApplyIBufBitmapInit(const LogEntry &log, Page *page);
#endif
