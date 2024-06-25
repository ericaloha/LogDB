#include <dlfcn.h>
#include <cstdarg>
#include <unordered_map>
#include <unordered_set>
#include <cstdio>
#include <string>
#include <vector>
#include <list>
#include <shared_mutex>
#include <memory>
#include <cstring>
#include <mutex>
#include <cassert>
#include <numeric>
#include <algorithm>
#define SYSBENCH
using space_id_t = size_t;
using page_id_t = size_t;
using page_size_t = size_t;
using frame_id_t = size_t;
using byte = char;

//class Status {
// public:
//  ~Status() {
//    printf("total_read: %lu\n", total_read);
//    printf("total_write: %lu\n", total_write);
//    auto total_call = read_latency.size();
//
//    std::sort(read_latency.begin(), read_latency.end());
//    auto sum_latency = std::accumulate(read_latency.begin(), read_latency.end(), (uint64_t)0);
//    auto avg_latency = sum_latency / total_call;
//    auto min_latency = read_latency[0];
//    auto max_latency = read_latency[total_call - 1];
//    printf("performed %zu pread call, min latency: %d, max latency: %d, avg latency: %lu\n",
//           total_call, min_latency, max_latency, avg_latency);
//
//    auto total_call95 = static_cast<int>((static_cast<double>(total_call) * 0.95));
//    auto sum_latency95 = std::accumulate(read_latency.begin(),
//                                         read_latency.begin() + total_call95,
//                                         (uint64_t)0);
//    auto avg_latency95 = sum_latency95 / total_call95;
//    auto min_latency95 = read_latency[0];
//    auto max_latency95 = read_latency[total_call95 - 1];
//    printf("min latency of 95%% calls: %d, max latency of 95%% calls: %d, avg latency of 95%% calls: %lu\n",
//           min_latency95, max_latency95, avg_latency95);
//
//  }
//  std::mutex lock;
//  uint64_t total_read {};
//  uint64_t total_write {};
//  std::vector<int> read_latency {};
//};

//Status status;

static constexpr const page_size_t PAGE_SIZE = 16384;
static constexpr uint32_t FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID = 34;


inline uint32_t mach_read_from_4(const byte* b) {
  return (static_cast<uint32_t>(b[0]) << 24)
      | (static_cast<uint32_t>(b[1]) << 16)
      | (static_cast<uint32_t>(b[2]) << 8)
      | static_cast<uint32_t>(b[3]);
}

static std::unordered_map<int, std::string> fd2filename;
static std::unordered_map<int, space_id_t> fd2space_id;
static std::unordered_set<std::string> logfile_set {
    "./iblogfile0",
    "./iblogfile1"
};

//local buffer for data pages correponding to different file and offset
std::unordered_map<int, std::unordered_map<off_t, char*>*> local_buf; // fd:<offset, 16K buf>

#ifdef TPCC
static std::unordered_set<std::string> datafile_set {
        "./tpcc/customer.ibd",
        "./tpcc/district.ibd",
        "./tpcc/history.ibd",
        "./tpcc/item.ibd",
        "./tpcc/new_orders.ibd",
        "./tpcc/order_line.ibd",
        "./tpcc/orders.ibd",
        "./tpcc/stock.ibd",
        "./tpcc/warehouse.ibd"
};
#else
static std::unordered_set<std::string> datafile_set {
    "./sbtest/sbtest1.ibd",
    "./sbtest/sbtest2.ibd",
    "./sbtest/sbtest3.ibd",
    "./sbtest/sbtest4.ibd",
    "./sbtest/sbtest5.ibd",
    "./sbtest/sbtest6.ibd",
    "./sbtest/sbtest7.ibd",
    "./sbtest/sbtest8.ibd",
    "./sbtest/sbtest9.ibd",
    "./sbtest/sbtest10.ibd",
    "./sbtest/sbtest11.ibd",
    "./sbtest/sbtest12.ibd",
    "./sbtest/sbtest13.ibd",
    "./sbtest/sbtest14.ibd",
    "./sbtest/sbtest15.ibd",
    "./sbtest/sbtest16.ibd",
    "./sbtest/sbtest17.ibd",
    "./sbtest/sbtest18.ibd",
    "./sbtest/sbtest19.ibd",
    "./sbtest/sbtest20.ibd",
    "./sbtest/sbtest21.ibd",
    "./sbtest/sbtest22.ibd",
    "./sbtest/sbtest23.ibd",
    "./sbtest/sbtest24.ibd",
    "./sbtest/sbtest25.ibd",
    "./sbtest/sbtest26.ibd",
    "./sbtest/sbtest27.ibd",
    "./sbtest/sbtest28.ibd",
    "./sbtest/sbtest29.ibd",
    "./sbtest/sbtest30.ibd",
    "./sbtest/sbtest31.ibd",
    "./sbtest/sbtest32.ibd",
    "./sbtest/sbtest33.ibd",
    "./sbtest/sbtest34.ibd",
    "./sbtest/sbtest35.ibd",
    "./sbtest/sbtest36.ibd",
    "./sbtest/sbtest37.ibd",
    "./sbtest/sbtest38.ibd",
    "./sbtest/sbtest39.ibd",
    "./sbtest/sbtest40.ibd"
};
#endif
static bool is_data_file(int fd) {

  if (auto iter1 = fd2filename.find(fd); iter1 != fd2filename.end()) {
    if (auto iter2 = datafile_set.find(iter1->second); iter2 != datafile_set.end()) {
      return true;
    }
  }

  return false;
}

static bool is_data_file(const std::string &filename) {

  if (auto iter = datafile_set.find(filename); iter != datafile_set.end()) {
    return true;
  }

  return false;
}

extern "C" {

typedef int (*orig_open_f_type)(const char *pathname, int flags, ...);
typedef int (*orig_open64_f_type)(const char *pathname, int flags, ...);
typedef ssize_t (*orig_pread_f_type)(int fd, void *buf, size_t count, off_t offset);
typedef ssize_t (*orig_pread64_f_type)(int fd, void *buf, size_t count, off_t offset);
typedef ssize_t (*orig_pwrite_f_type)(int fd, const void *buf, size_t count, off_t offset);
typedef ssize_t (*orig_pwrite64_f_type)(int fd, const void *buf, size_t count, off_t offset);
typedef int (*orig_close_f_type)(int fd);

static orig_open_f_type orig_open = (orig_open_f_type)dlsym(RTLD_NEXT, "open");
static orig_open_f_type orig_open64 = (orig_open64_f_type)dlsym(RTLD_NEXT, "open64");
static orig_pread_f_type orig_pread = (orig_pread_f_type)dlsym(RTLD_NEXT, "pread");
static orig_pread_f_type orig_pread64 = (orig_pread64_f_type)dlsym(RTLD_NEXT, "pread64");
static orig_pwrite_f_type orig_pwrite = (orig_pwrite_f_type)dlsym(RTLD_NEXT, "pwrite");
static orig_pwrite_f_type orig_pwrite64 = (orig_pwrite64_f_type)dlsym(RTLD_NEXT, "pwrite64");
static orig_close_f_type orig_close = (orig_close_f_type)dlsym(RTLD_NEXT, "close");

int open(const char *pathname, int flags, ...) {
  va_list args;
  // 初始化可变参数列表
  va_start(args, flags);
  auto fd = orig_open(pathname, flags, va_arg(args, mode_t));
  va_end(args);
#ifdef LOG__TRACE
  printf("%-10s %-30s %-10s %-10s\n", "open", pathname, "", "");
#endif
  fd2filename[fd] = pathname;

  if (is_data_file(pathname)) {
    byte first_page_buf[PAGE_SIZE];
    orig_pread(fd, first_page_buf, PAGE_SIZE, 0);
    space_id_t space_id = mach_read_from_4(first_page_buf + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
    fd2space_id[fd] = space_id;
    //register into local data buffer
    std::unordered_map<off_t, char*>* tmp_map = new std::unordered_map<off_t, char*>;
    local_buf.emplace(fd,tmp_map);

//    printf("%s -> %zu\n", pathname, space_id);
  }
//  printf("open %s\n", pathname);
  return fd;
}

int open64(const char *pathname, int flags, ...) {
//  printf("opening %s\n", pathname);
  va_list args;
  // 初始化可变参数列表
  va_start(args, flags);
  auto fd = orig_open64(pathname, flags, va_arg(args, mode_t));
  va_end(args);
#ifdef LOG__TRACE
//  printf("%-10s %-30s %-10s %-10s\n", "open64", pathname, "", "");
#endif
  fd2filename[fd] = pathname;

  if (is_data_file(pathname)) {
    byte first_page_buf[PAGE_SIZE];
    orig_pread(fd, first_page_buf, PAGE_SIZE, 0);
    space_id_t space_id = mach_read_from_4(first_page_buf + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID);
    fd2space_id[fd] = space_id;
    //register into local data buffer
    std::unordered_map<off_t, char*>* tmp_map = new std::unordered_map<off_t, char*>;
    local_buf.emplace(fd,tmp_map);
//    printf("%s -> %zu\n", pathname, space_id);
  }
//  printf("opened %s\n", pathname);
  return fd;
}

ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
//  auto st = std::chrono::steady_clock::now();
#ifdef LOG__TRACE
  printf("%-10s %-30s %-10zu %-10ld\n", "pread", fd2filename[fd].c_str(), count, offset);
#endif
  //first check fromlocal buf
  if(local_buf.find(fd)!=local_buf.end()){
    std::unordered_map<off_t, char*>* tmp = local_buf[fd];
    if(tmp->find(offset)!=tmp->end()){
      char* tmp_buf = (*tmp)[offset];
      memcpy(buf,tmp_buf,count);
      return count;
    }
  }

  auto sz = orig_pread(fd, buf, count, offset);
//  auto ed = std::chrono::steady_clock::now();
//  status.lock.lock();
//  status.read_latency.emplace_back((ed - st).count());
//  status.total_read += sz;
//  status.lock.unlock();
  return sz;
}

ssize_t pread64(int fd, void *buf, size_t count, off_t offset) {
//  auto st = std::chrono::steady_clock::now();
#ifdef LOG__TRACE
  printf("%-10s %-30s %-10zu %-10ld\n", "pread64", fd2filename[fd].c_str(), count, offset);
#endif
  //first check fromlocal buf
  if(local_buf.find(fd)!=local_buf.end()){
    std::unordered_map<off_t, char*>* tmp = local_buf[fd];
    if(tmp->find(offset)!=tmp->end()){
      char* tmp_buf = (*tmp)[offset];
      memcpy(buf,tmp_buf,count);
      return count;
    }
  }
  auto sz = orig_pread64(fd, buf, count, offset);
//  auto ed = std::chrono::steady_clock::now();
//  status.lock.lock();
//  status.read_latency.emplace_back((ed - st).count());
//  status.total_read += sz;
//  status.lock.unlock();
  return sz;
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
  // if data page write, just return
  if (is_data_file(fd)) {
    //insert page into lcoal buf
    if(local_buf.find(fd)==local_buf.end()){
      printf("hkc-test fd error!\n");
    }else{
      std::unordered_map<off_t, char*>* tmp = local_buf[fd];
      if(tmp->find(offset)!=tmp->end()){
        //update
        char* tmp_buf = (*tmp)[offset];
        memcpy(tmp_buf,buf,count);
      }else{
        //insert
        char* tmp_buf = (char*)malloc(sizeof(char)*count);
        memcpy(tmp_buf,buf,count);
        tmp->emplace(offset,tmp_buf);
      }
    }
    return static_cast<ssize_t>(count);
  }
  // otherwise, call the original pwrite function
#ifdef LOG__TRACE
  printf("%-10s %-30s %-10zu %-10ld\n", "pwrite", fd2filename[fd].c_str(), count, offset);
#endif
  auto sz = orig_pwrite(fd, buf, count, offset);

//  status.lock.lock();
//  status.total_write += sz;
//  status.lock.unlock();

  return sz;
}

ssize_t pwrite64(int fd, const void *buf, size_t count, off_t offset) {
  // if data page write, just return
  if (is_data_file(fd)) {

    //insert page into lcoal buf
    if(local_buf.find(fd)==local_buf.end()){
      printf("hkc-test fd error!\n");
    }else{
      std::unordered_map<off_t, char*>* tmp = local_buf[fd];
      if(tmp->find(offset)!=tmp->end()){
        //update
        char* tmp_buf = (*tmp)[offset];
        memcpy(tmp_buf,buf,count);
      }else{
        //insert
        char* tmp_buf = (char*)malloc(sizeof(char)*count);
        memcpy(tmp_buf,buf,count);
        tmp->emplace(offset,tmp_buf);
      }
    }

    return static_cast<ssize_t>(count);
  }
  // otherwise, call the original pwrite function
#ifdef LOG__TRACE
  printf("%-10s %-30s %-10zu %-10ld\n", "pwrite64", fd2filename[fd].c_str(), count, offset);
#endif
  auto sz = orig_pwrite64(fd, buf, count, offset);

//  status.lock.lock();
//  status.total_write += sz;
//  status.lock.unlock();

  return sz;
}

ssize_t close(int fd) {
#ifdef LOG__TRACE
  printf("%-10s %-30s %-10s %-10s\n", "close", fd2filename[fd].c_str(), "", "");
#endif
  if (is_data_file(fd)) {
    fd2space_id.erase(fd);
    //reclaim local buffer space
    if(local_buf.find(fd)==local_buf.end()){
      printf("hkc test close error\n");
    }else{
      std::unordered_map<off_t, char*>* tmp = local_buf[fd];
      for (std::unordered_map<off_t, char*>::iterator it = tmp->begin();it!=tmp->end();it++){
        char* tmp_buf = it->second;
        free(tmp_buf);
      }
      delete tmp;
    }
  }
  fd2filename.erase(fd);

  return orig_close(fd);
}

}


