#include <dlfcn.h>
#include <cstdarg>
#include <cstdio>
#include <string>
#include <memory>
#include <vector>
#include <chrono>
#include <mutex>
#include <algorithm>
#include <numeric>
#include <thread>
class Status {
 public:
  ~Status() {
    printf("total_read: %lu\n", total_read);
    printf("total_write: %lu\n", total_write);
    auto total_call = read_latency.size();

    std::sort(read_latency.begin(), read_latency.end());
    auto sum_latency = std::accumulate(read_latency.begin(), read_latency.end(), (uint64_t)0);
    auto avg_latency = sum_latency / total_call;
    auto min_latency = read_latency[0];
    auto max_latency = read_latency[total_call - 1];
    printf("performed %zu pread call, min latency: %d, max latency: %d, avg latency: %lu\n",
           total_call, min_latency, max_latency, avg_latency);

    auto total_call95 = static_cast<int>((static_cast<double>(total_call) * 0.95));
    auto sum_latency95 = std::accumulate(read_latency.begin(),
                                         read_latency.begin() + total_call95,
                                         (uint64_t)0);
    auto avg_latency95 = sum_latency95 / total_call95;
    auto min_latency95 = read_latency[0];
    auto max_latency95 = read_latency[total_call95 - 1];
    printf("min latency of 95%% calls: %d, max latency of 95%% calls: %d, avg latency of 95%% calls: %lu\n",
           min_latency95, max_latency95, avg_latency95);

  }
  std::mutex lock;
  uint64_t total_read {};
  uint64_t total_write {};
  std::vector<int> read_latency {};
};

Status status;

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
  return fd;
}

int open64(const char *pathname, int flags, ...) {
  va_list args;
  // 初始化可变参数列表
  va_start(args, flags);
  auto fd = orig_open64(pathname, flags, va_arg(args, mode_t));
  va_end(args);
  return fd;
}

ssize_t pread(int fd, void *buf, size_t count, off_t offset) {

//  std::this_thread::sleep_for(std::chrono::milliseconds(8));
  auto st = std::chrono::steady_clock::now();
  auto ret_val = orig_pread(fd, buf, count, offset);
  auto ed = std::chrono::steady_clock::now();

  status.lock.lock();
  status.total_read += count;
  status.read_latency.emplace_back((ed - st).count());
  status.lock.unlock();

  return ret_val;
}

ssize_t pread64(int fd, void *buf, size_t count, off_t offset) {

//  std::this_thread::sleep_for(std::chrono::milliseconds(8));
  auto st = std::chrono::steady_clock::now();
  auto ret_val = orig_pread64(fd, buf, count, offset);
  auto ed = std::chrono::steady_clock::now();

  status.lock.lock();
  status.total_read += count;
  status.read_latency.emplace_back((ed - st).count());
  status.lock.unlock();

  return ret_val;
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {

  auto ret_val = orig_pwrite(fd, buf, count, offset);

  status.lock.lock();
  status.total_write += count;
  status.lock.unlock();

  return ret_val;
}

ssize_t pwrite64(int fd, const void *buf, size_t count, off_t offset) {
  auto ret_val = orig_pwrite64(fd, buf, count, offset);

  status.lock.lock();
  status.total_write += count;
  status.lock.unlock();

  return ret_val;
}

ssize_t close(int fd) {
  return orig_close(fd);
}

}

