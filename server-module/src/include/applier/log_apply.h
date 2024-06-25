#pragma once
#include <unordered_map>
#include <list>
#include <string>
#include "applier/buffer_pool.h"
#include "applier/bean.h"
#include "applier/log_log.h"
#include "applier/applier_config.h"

// 启动applier线程，n_thread指明有多少applier线程，其中1个scheduler，1个worker
void log_apply_thread_start(int n_thread);

void log_apply_do_apply(const PageAddress &page_address, std::list<LogEntry> *log_entry_list);
