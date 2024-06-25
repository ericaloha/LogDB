#include <dirent.h>
#include <iostream>
#include <cassert>
#include "applier/utility.h"
/**
 * 递归遍历文件夹下以suffix结尾的文件
 * @param dir_path 文件夹路径，不要以/结尾
 * @param suffix 文件结尾
 * @return path目录下以suffix结尾的所有文件名
 */
bool TravelDirectory(const std::string &dir_path, const std::string &suffix, std::vector<std::string> &files) {
    DIR *d;
    struct dirent *file;
    if(!(d = opendir(dir_path.data()))) {
        std::cerr << "open directory " << dir_path << " failed!!!" << std::endl;
        exit(1);
        closedir(d);
        return false;
    }

    while((file = readdir(d)) != nullptr) {
        std::string file_name(file->d_name);
        // 跳过当前目录（.）和上一级目录（..）
        if(file_name.empty() || file_name == "." || file_name == "..") {
            continue;
        }
        // 如果是普通的文件
        if(file->d_type == 8) {
            // 跳过不是以suffix结尾的文件
            if (suffix.size() > file_name.size()
                || file_name.substr(file_name.size() - suffix.size()) != suffix) {
                continue;
            }
            std::string path(dir_path);
            path.append("/").append(file_name);
            files.push_back(path);
        }
        //如果是文件夹
        if(file->d_type == 4) {
            std::string sub_dir_path;
            sub_dir_path.append(dir_path).append("/").append(file_name);
            TravelDirectory(sub_dir_path, suffix, files);
        }
    }
    closedir(d);
    return true;
}

const char* GetLogString(LOG_TYPE type) {
    switch (type) {
        case LOG_TYPE::MLOG_SINGLE_REC_FLAG:
            return("MLOG_SINGLE_REC_FLAG");

        case LOG_TYPE::MLOG_1BYTE:
            return("MLOG_1BYTE");

        case LOG_TYPE::MLOG_2BYTES:
            return("MLOG_2BYTES");

        case LOG_TYPE::MLOG_4BYTES:
            return("MLOG_4BYTES");

        case LOG_TYPE::MLOG_8BYTES:
            return("MLOG_8BYTES");

        case LOG_TYPE::MLOG_REC_INSERT:
            return("MLOG_REC_INSERT");

        case LOG_TYPE::MLOG_REC_CLUST_DELETE_MARK:
            return("MLOG_REC_CLUST_DELETE_MARK");

        case LOG_TYPE::MLOG_REC_SEC_DELETE_MARK:
            return("MLOG_REC_SEC_DELETE_MARK");

        case LOG_TYPE::MLOG_REC_UPDATE_IN_PLACE:
            return("MLOG_REC_UPDATE_IN_PLACE");

        case LOG_TYPE::MLOG_REC_DELETE:
            return("MLOG_REC_DELETE");

        case LOG_TYPE::MLOG_LIST_END_DELETE:
            return("MLOG_LIST_END_DELETE");

        case LOG_TYPE::MLOG_LIST_START_DELETE:
            return("MLOG_LIST_START_DELETE");

        case LOG_TYPE::MLOG_LIST_END_COPY_CREATED:
            return("MLOG_LIST_END_COPY_CREATED");

        case LOG_TYPE::MLOG_PAGE_REORGANIZE:
            return("MLOG_PAGE_REORGANIZE");

        case LOG_TYPE::MLOG_PAGE_CREATE:
            return("MLOG_PAGE_CREATE");

        case LOG_TYPE::MLOG_UNDO_INSERT:
            return("MLOG_UNDO_INSERT");

        case LOG_TYPE::MLOG_UNDO_ERASE_END:
            return("MLOG_UNDO_ERASE_END");

        case LOG_TYPE::MLOG_UNDO_INIT:
            return("MLOG_UNDO_INIT");

        case LOG_TYPE::MLOG_UNDO_HDR_DISCARD:
            return("MLOG_UNDO_HDR_DISCARD");

        case LOG_TYPE::MLOG_UNDO_HDR_REUSE:
            return("MLOG_UNDO_HDR_REUSE");

        case LOG_TYPE::MLOG_UNDO_HDR_CREATE:
            return("MLOG_UNDO_HDR_CREATE");

        case LOG_TYPE::MLOG_REC_MIN_MARK:
            return("MLOG_REC_MIN_MARK");

        case LOG_TYPE::MLOG_IBUF_BITMAP_INIT:
            return("MLOG_IBUF_BITMAP_INIT");

        case LOG_TYPE::MLOG_INIT_FILE_PAGE:
            return("MLOG_INIT_FILE_PAGE");

        case LOG_TYPE::MLOG_WRITE_STRING:
            return("MLOG_WRITE_STRING");

        case LOG_TYPE::MLOG_MULTI_REC_END:
            return("MLOG_MULTI_REC_END");

        case LOG_TYPE::MLOG_DUMMY_RECORD:
            return("MLOG_DUMMY_RECORD");

        case LOG_TYPE::MLOG_FILE_DELETE:
            return("MLOG_FILE_DELETE");

        case LOG_TYPE::MLOG_COMP_REC_MIN_MARK:
            return("MLOG_COMP_REC_MIN_MARK");

        case LOG_TYPE::MLOG_COMP_PAGE_CREATE:
            return("MLOG_COMP_PAGE_CREATE");

        case LOG_TYPE::MLOG_COMP_REC_INSERT:
            return("MLOG_COMP_REC_INSERT");

        case LOG_TYPE::MLOG_COMP_REC_CLUST_DELETE_MARK:
            return("MLOG_COMP_REC_CLUST_DELETE_MARK");

        case LOG_TYPE::MLOG_COMP_REC_SEC_DELETE_MARK:
            return("MLOG_COMP_REC_SEC_DELETE_MARK");

        case LOG_TYPE::MLOG_COMP_REC_UPDATE_IN_PLACE:
            return("MLOG_COMP_REC_UPDATE_IN_PLACE");

        case LOG_TYPE::MLOG_COMP_REC_DELETE:
            return("MLOG_COMP_REC_DELETE");

        case LOG_TYPE::MLOG_COMP_LIST_END_DELETE:
            return("MLOG_COMP_LIST_END_DELETE");

        case LOG_TYPE::MLOG_COMP_LIST_START_DELETE:
            return("MLOG_COMP_LIST_START_DELETE");

        case LOG_TYPE::MLOG_COMP_LIST_END_COPY_CREATED:
            return("MLOG_COMP_LIST_END_COPY_CREATED");

        case LOG_TYPE::MLOG_COMP_PAGE_REORGANIZE:
            return("MLOG_COMP_PAGE_REORGANIZE");

        case LOG_TYPE::MLOG_FILE_CREATE2:
            return("MLOG_FILE_CREATE2");

        case LOG_TYPE::MLOG_ZIP_WRITE_NODE_PTR:
            return("MLOG_ZIP_WRITE_NODE_PTR");

        case LOG_TYPE::MLOG_ZIP_WRITE_BLOB_PTR:
            return("MLOG_ZIP_WRITE_BLOB_PTR");

        case LOG_TYPE::MLOG_ZIP_WRITE_HEADER:
            return("MLOG_ZIP_WRITE_HEADER");

        case LOG_TYPE::MLOG_ZIP_PAGE_COMPRESS:
            return("MLOG_ZIP_PAGE_COMPRESS");

        case LOG_TYPE::MLOG_ZIP_PAGE_COMPRESS_NO_DATA:
            return("MLOG_ZIP_PAGE_COMPRESS_NO_DATA");

        case LOG_TYPE::MLOG_ZIP_PAGE_REORGANIZE:
            return("MLOG_ZIP_PAGE_REORGANIZE");

        case LOG_TYPE::MLOG_FILE_RENAME2:
            return("MLOG_FILE_RENAME2");

        case LOG_TYPE::MLOG_FILE_NAME:
            return("MLOG_FILE_NAME");

        case LOG_TYPE::MLOG_CHECKPOINT:
            return("MLOG_CHECKPOINT");

        case LOG_TYPE::MLOG_PAGE_CREATE_RTREE:
            return("MLOG_PAGE_CREATE_RTREE");

        case LOG_TYPE::MLOG_COMP_PAGE_CREATE_RTREE:
            return("MLOG_COMP_PAGE_CREATE_RTREE");

        case LOG_TYPE::MLOG_INIT_FILE_PAGE2:
            return("MLOG_INIT_FILE_PAGE2");

        case LOG_TYPE::MLOG_INDEX_LOAD:
            return("MLOG_INDEX_LOAD");

        case LOG_TYPE::MLOG_TRUNCATE:
            return("MLOG_TRUNCATE");

        default:
            assert(false);
            return("UNKNOWN_TYPE");
    }
}

uint32_t mach_parse_compressed(const byte **ptr, const byte* end_ptr) {
    if (*ptr >= end_ptr) {
        *ptr = nullptr;
        return 0;
    }

    auto val = static_cast<uint32_t>(mach_read_from_1(*ptr));

    if (val < 0x80) {
        /* 0nnnnnnn (7 bits) */
        ++*ptr;
        return val;
    }
#if defined(__GNUC__) && (__GNUC__ >= 5) && !defined(__clang__)
#define DEPLOY_FENCE
#endif

#ifdef DEPLOY_FENCE
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
#endif
    if (val < 0xC0) {
        /* 10nnnnnn nnnnnnnn (14 bits) */
        if (end_ptr >= *ptr + 2) {
            val = static_cast<uint32_t>(mach_read_from_2(*ptr)) & 0x3FFF;
            assert(val > 0x7F);
            *ptr += 2;
            return val;
        }
        *ptr = nullptr;
        return 0;
    }
#ifdef DEPLOY_FENCE
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
#endif
    if (val < 0xE0) {
        /* 110nnnnn nnnnnnnn nnnnnnnn (21 bits) */
        if (end_ptr >= *ptr + 3) {
            val = mach_read_from_3(*ptr) & 0x1FFFFF;
            assert(val > 0x3FFF);
            *ptr += 3;
            return val;
        }
        *ptr = nullptr;
        return 0;
    }
#ifdef DEPLOY_FENCE
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
#endif
    if (val < 0xF0) {
        /* 1110nnnn nnnnnnnn nnnnnnnn nnnnnnnn (28 bits) */
        if (end_ptr >= *ptr + 4) {
            val = mach_read_from_4(*ptr) & 0xFFFFFFF;
            assert(val > 0x1FFFFF);
            *ptr += 4;
            return val;
        }
        *ptr = nullptr;
        return 0;
    }
#ifdef DEPLOY_FENCE
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
#endif

#undef DEPLOY_FENCE
    assert(val == 0xF0);

    /* 11110000 nnnnnnnn nnnnnnnn nnnnnnnn nnnnnnnn (32 bits) */
    if (end_ptr >= *ptr + 5) {
        val = mach_read_from_4(*ptr + 1);
        assert(val > 0xFFFFFFF);
        *ptr += 5;
        return val;
    }

    *ptr = nullptr;
    return 0;
}

uint32_t mach_read_next_compressed(const byte**	b) {
    auto val = static_cast<uint32_t>(mach_read_from_1(*b));

    if (val < 0x80) {
        /* 0nnnnnnn (7 bits) */
        ++*b;
    } else if (val < 0xC0) {
        /* 10nnnnnn nnnnnnnn (14 bits) */
        val = mach_read_from_2(*b) & 0x3FFF;
        assert(val > 0x7F);
        *b += 2;
    } else if (val < 0xE0) {
        /* 110nnnnn nnnnnnnn nnnnnnnn (21 bits) */
        val = mach_read_from_3(*b) & 0x1FFFFF;
        assert(val > 0x3FFF);
        *b += 3;
    } else if (val < 0xF0) {
        /* 1110nnnn nnnnnnnn nnnnnnnn nnnnnnnn (28 bits) */
        val = mach_read_from_4(*b) & 0xFFFFFFF;
        assert(val > 0x1FFFFF);
        *b += 4;
    } else {
        /* 11110000 nnnnnnnn nnnnnnnn nnnnnnnn nnnnnnnn (32 bits) */
        assert(val == 0xF0);
        val = mach_read_from_4(*b + 1);
        assert(val > 0xFFFFFFF);
        *b += 5;
    }

    return val;
}

uint64_t mach_u64_parse_compressed(const byte**	ptr, const byte*	end_ptr) {
    uint64_t val = 0;

    if (end_ptr < *ptr + 5) {
        *ptr = nullptr;
        return val;
    }

    val = mach_read_next_compressed(ptr);

    if (end_ptr < *ptr + 4) {
        *ptr = nullptr;
        return val;
    }

    val <<= 32;
    val |= mach_read_from_4(*ptr);
    *ptr += 4;

    return val;
}

/*******************************************************//**
Calculates the new value for lsn when more data is added to the log. */
lsn_t recv_calc_lsn_on_data_add(
/*======================*/
        lsn_t		lsn,	/*!< in: old lsn */
        uint64_t 	len)	/*!< in: this many bytes of data is
				added, log block headers not included */
{
    uint32_t frag_len;
    uint64_t lsn_len;

    frag_len = (lsn % OS_FILE_LOG_BLOCK_SIZE) - LOG_BLOCK_HDR_SIZE;
    assert(frag_len < OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE
                      - LOG_BLOCK_TRL_SIZE);
    lsn_len = len;
    lsn_len += (lsn_len + frag_len)
               / (OS_FILE_LOG_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE
                  - LOG_BLOCK_TRL_SIZE)
               * (LOG_BLOCK_HDR_SIZE + LOG_BLOCK_TRL_SIZE);

    return lsn + lsn_len;
}