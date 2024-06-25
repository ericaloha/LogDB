#pragma once
#include <cinttypes>
#include "applier/applier_config.h"
#include "applier/bean.h"

uint32_t
rec_get_status(
/*===========*/
    const byte*	rec);

uint32_t
rec_get_bit_field_1(const byte*	rec, uint32_t offs, uint32_t mask, uint32_t shift);

bool rec_info_bits_valid(uint32_t bits);

void rec_set_bit_field_1(byte*	rec, uint32_t	val, uint32_t	offs, uint32_t mask, uint32_t shift);
void
rec_set_bit_field_2(
/*================*/
    byte*	rec,	/*!< in: pointer to record origin */
    uint32_t 	val,	/*!< in: value to set */
    uint32_t	offs,	/*!< in: offset from the origin down */
    uint32_t	mask,	/*!< in: mask used to filter bits */
    uint32_t	shift);
uint32_t rec_get_info_bits(const byte* rec, bool comp);

void rec_set_info_bits_new(byte*	rec, uint32_t bits);

uint32_t rec_get_info_and_status_bits(const byte*	rec);

void rec_set_status(byte*	rec, uint32_t bits);

void rec_set_info_and_status_bits(byte*	rec, uint32_t bits);

uint32_t
rec_get_bit_field_2(
/*================*/
    const byte*	rec,	/*!< in: pointer to record origin */
    uint32_t offs,	/*!< in: offset from the origin down */
    uint32_t mask,	/*!< in: mask used to filter bits */
    uint32_t shift);
/******************************************************//**
The following function is used to get the order number
of a new-style record in the heap of the index page.
@return heap order number */
uint32_t rec_get_heap_no_new(const byte*	rec);


const byte*
rec_get_next_ptr_const(const byte *page, const byte* rec);

byte*
rec_get_next_ptr(const byte *page, byte* rec);

byte*
rec_copy(
    void*		buf,
    const RecordInfo &rec_info);

// 拿到next_ptr相对于page的偏移量
uint32_t rec_get_next_offs(const byte* page, const byte *rec);

void
rec_set_next_offs_new(
/*==================*/
    const byte* page,
    byte*	rec,	/*!< in/out: new-style physical record */
    uint32_t 	next);

void rec_set_n_owned_new(
/*================*/
    byte*		rec,
    uint32_t n_owned);

void
rec_set_heap_no_new(
/*================*/
    byte*	rec,	/*!< in/out: physical record */
    uint32_t heap_no);


void
rec_set_deleted_flag_new(
/*=====================*/
    byte* rec,	/*!< in/out: new-style physical record */
    uint32_t flag);

uint32_t
rec_get_nth_field_offs(
/*===================*/
    const RecordInfo &rec_info,/*!< in: array returned by rec_get_offsets() */
    uint32_t		n,	/*!< in: index of the field */
    uint32_t*		len);

byte *rec_get_nth_field(byte *rec,
                        const RecordInfo &rec_info,/*!< in: array returned by rec_get_offsets() */
                        uint32_t		n,	/*!< in: index of the field */
                        uint32_t*		len);

bool rec_get_1byte_offs_flag(const byte*	rec);

uint32_t rec_1_get_prev_field_end_info(const byte*	rec, uint32_t n);

uint32_t rec_1_get_field_start_offs(const byte*	rec,uint32_t n);

uint32_t rec_2_get_prev_field_end_info(const byte*	rec, uint32_t n);

uint32_t rec_2_get_field_start_offs(const byte*	rec, uint32_t n);

uint32_t rec_get_field_start_offs(const byte*	rec, uint32_t n);

uint32_t rec_get_nth_field_size(const byte*	rec, uint32_t n);

uint32_t rec_1_get_field_end_info(const byte*	rec, uint32_t n);

void rec_1_set_field_end_info(byte* rec, uint32_t n, uint32_t info);

uint32_t rec_2_get_field_end_info(const byte* rec, uint32_t n);

void rec_2_set_field_end_info(byte* rec, uint32_t n, uint32_t info);

void rec_set_nth_field_null_bit(byte* rec, uint32_t i, bool val);

void rec_set_nth_field_sql_null(byte* rec, uint32_t n);

uint32_t rec_offs_nth_sql_null(const RecordInfo &rec_info, uint32_t n);

void rec_set_nth_field(byte* rec, const RecordInfo& rec_info,
                       uint32_t n, const void*	data, uint32_t len);

uint32_t rec_get_deleted_flag(const byte*	rec);

