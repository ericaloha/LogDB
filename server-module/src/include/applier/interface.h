#ifndef APPLIER_APPLIER_H_
#define APPLIER_APPLIER_H_
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

// forward declaratio
struct fsal_obj_handle;

/**
 * check if the specific file is a log file
 * @param filename
 * @return if is a log file, return the its index in log group, or return -1 if not.
 */
extern int is_log_file_in_name(const char *filename);

extern int is_ibd_file_in_name(const char *filename);
extern int is_ibd_file_in_handle(const struct fsal_obj_handle *handle);
/**
 * check if the specific file is a log file
 * @param handle vfs_file_handle_t
 * @return if is a log file, return the its index in log group, or return -1 if not.
 */
extern int is_log_file_in_handle(const struct fsal_obj_handle *handle);

extern void register_log_file_handle(int index, struct fsal_obj_handle *handle);
extern void register_ibd_file_handle(struct fsal_obj_handle *handle, int space_id);
extern void init_applier_module(void);
extern void wait_until_apply_done(int space_id, uint64_t offset, size_t io_amount);
/**
 * 把 log writer 写的日志拷贝到log group的buf里面去
 * @param log_file_index log writer要写第几个log文件
 * @param offset log writer要写到的起始offset
 * @param iov
 * @param iov_count
 */
extern void copy_log_to_buf(int log_file_index, size_t offset, struct iovec iov[], int iov_count);

extern void copy_page_to_buf(char *dest_buf, uint32_t space_id, uint32_t start_page_id, int n_pages);
#ifdef __cplusplus
}
#endif

#endif