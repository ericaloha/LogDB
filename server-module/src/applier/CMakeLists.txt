SET(Applier_STAT_SRCS
        log_parse.cpp
        log_log.cpp
        record.cpp
        bean.cpp
        utility.cpp
        buffer_pool.cpp
        log_apply.cpp
        interface.cpp)

add_library(Applier OBJECT ${Applier_STAT_SRCS})
add_sanitizers(Applier)
set_target_properties(Applier PROPERTIES COMPILE_FLAGS "-fPIC")
link_directories(/usr/local/lib)
target_link_libraries(rocksdb)