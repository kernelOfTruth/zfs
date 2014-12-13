/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
 * or http://www.opensolaris.org/os/licensing.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL HEADER in each
 * file and include the License file at usr/src/OPENSOLARIS.LICENSE.
 * If applicable, add the following below this CDDL HEADER, with the
 * fields enclosed by brackets "[]" replaced with your own identifying
 * information: Portions Copyright [yyyy] [name of copyright owner]
 *
 * CDDL HEADER END
 */

/*
 * Copyright (c) 2014 by Chunwei Chen. All rights reserved.
 */

/*
 * ABD - ARC buffer data
 * ABD is an abstract data structure for ARC. There are two types of ABD:
 * linear for metadata and scatter for data.
 * Their type is determined by the lowest bit of abd_t pointer.
 * The public API will automatically determine the type
 */

#ifndef _ABD_H
#define	_ABD_H

#include <sys/zfs_context.h>

#ifdef __cplusplus
extern "C" {
#endif

#define	ARC_BUF_DATA_MAGIC 0xa7cb0fda7aabdabd

/*
 * Convert an linear ABD to normal buffer
 */
#define	ABD_TO_BUF(abd)			\
(					\
{					\
	ASSERT_ABD_LINEAR(abd);		\
	(void *)(abd);			\
}					\
)

/*
 * Convert a normal buffer to linear ABD
 */
#define	BUF_TO_ABD(buf)			\
(					\
{					\
	abd_t *___abd = (abd_t *)buf;	\
	VERIFY(ABD_IS_LINEAR(___abd));	\
	___abd;				\
}					\
)


/* purely virtual structure to prevent dereferencing */
struct abd;
typedef struct abd abd_t;

#define	ABD_IS_SCATTER(abd)	(((unsigned long)abd) & 0x1)
#define	ABD_IS_LINEAR(abd)	(!ABD_IS_SCATTER(abd))
#define	ASSERT_ABD_SCATTER(abd)	ASSERT(ABD_IS_SCATTER(abd))
#define	ASSERT_ABD_LINEAR(abd)	ASSERT(ABD_IS_LINEAR(abd))

abd_t *abd_alloc(size_t size);
void abd_free(abd_t *, size_t);


/*
 * Macros to create public API to multiplex linear/scatter buffer
 */
#define	ABD_FUNC_WRAPPER(abd, func, ...)		\
do {							\
	if (ABD_IS_LINEAR(abd))				\
		u_##func(__VA_ARGS__);			\
	else						\
		do_##func(__VA_ARGS__);			\
} while (0)

#define	ABD_RET_FUNC_WRAPPER(type, abd, func, ...)	\
(							\
{							\
	type ___ret;					\
	if (ABD_IS_LINEAR(abd))				\
		___ret = u_##func(__VA_ARGS__);		\
	else						\
		___ret = do_##func(__VA_ARGS__);	\
	___ret;						\
}							\
)

#define	ABD_FUNC2_WRAPPER(abd1, abd2, func, ...)	\
do {							\
	if (ABD_IS_LINEAR(abd1)) {			\
		ASSERT_ABD_LINEAR(abd2);		\
		u_##func(__VA_ARGS__);			\
	} else {					\
		ASSERT_ABD_SCATTER(abd2);		\
		do_##func(__VA_ARGS__);			\
	}						\
} while (0)

#define	ABD_RET_FUNC2_WRAPPER(type, abd1, abd2, func, ...)\
(							\
{							\
	type ___ret;					\
	if (ABD_IS_LINEAR(abd1)) {			\
		ASSERT_ABD_LINEAR(abd2);		\
		___ret = u_##func(__VA_ARGS__);		\
	} else {					\
		ASSERT_ABD_SCATTER(abd2);		\
		___ret = do_##func(__VA_ARGS__);	\
	}						\
	___ret;						\
}							\
)

/*
 * ABD functions for scatter type
 * Should not be used directly
 */
void do_abd_iterate_rfunc(abd_t *, size_t,
    int (*)(const void *, uint64_t, void *), void *);
void do_abd_iterate_wfunc(abd_t *, size_t,
    int (*)(void *, uint64_t, void *), void *);
void do_abd_iterate_func2(abd_t *, abd_t *, size_t, size_t,
    int (*)(void *, void *, uint64_t, uint64_t, void *), void *);
void do_abd_copy(abd_t *, abd_t *, size_t);
void do_abd_copy_from_buf_off(abd_t *, const void *, size_t, size_t);
void do_abd_copy_to_buf_off(void *, abd_t *, size_t, size_t);
int do_abd_cmp(abd_t *, abd_t *, size_t);
int do_abd_cmp_buf_off(abd_t *, const void *, size_t, size_t);
void do_abd_zero_off(abd_t *, size_t, size_t);
#ifdef _KERNEL
int do_abd_copy_to_user_off(void __user *, abd_t *, size_t, size_t);
int do_abd_copy_from_user_off(abd_t *, const void __user *, size_t, size_t);
int do_abd_uiomove_off(abd_t *, size_t, enum uio_rw, uio_t *, size_t);
int do_abd_uiocopy_off(abd_t *, size_t, enum uio_rw, uio_t *, size_t *,
    size_t);
unsigned int do_abd_bio_map_off(struct bio *, abd_t *, unsigned int, size_t);
unsigned long do_abd_bio_nr_pages_off(abd_t *, unsigned int, size_t);
#endif
abd_t *do_abd_get_offset(abd_t *, size_t);
void do_abd_put_offset(abd_t *);
#define	do_abd_borrow_buf(a, n) zio_buf_alloc(n)
#define	do_abd_borrow_buf_copy(a, n)		\
(						\
{						\
	void *___b;				\
	___b = zio_buf_alloc(n);		\
	do_abd_copy_to_buf_off(___b, a, n, 0);	\
	___b;					\
}						\
)
#define	do_abd_return_buf(a, b, n) zio_buf_free(b, n)
#define	do_abd_return_buf_copy(a, b, n)		\
do {						\
	do_abd_copy_from_buf_off(a, b, n, 0);	\
	zio_buf_free(b, n);			\
} while (0)

/*
 * ABD functions for linear type
 * Should not be used directly
 */
#define	u_abd_iterate_rfunc(a, n, f, p) \
	(void) f(a, n, p)

#define	u_abd_iterate_wfunc(a, n, f, p) \
	(void) f(a, n, p)

#define	u_abd_iterate_func2(a, b, an, bn, f, p) \
	(void) f(a, b, an, bn, p)

#define	u_abd_copy(a, b, n) \
	(void) memcpy(a, b, n)

#define	u_abd_copy_from_buf_off(a, b, n, off) \
	(void) memcpy((void *)(a)+(off), b, n)

#define	u_abd_copy_to_buf_off(a, b, n, off) \
	(void) memcpy(a, (void *)(b)+(off), n)

#define	u_abd_cmp(a, b, n) \
	memcmp(a, b, n)

#define	u_abd_cmp_buf_off(a, b, n, off) \
	memcmp((void *)(a)+(off), b, n)

#define	u_abd_zero_off(a, n, off) \
	(void) memset((void *)(a)+(off), 0, n)

#define	u_abd_get_offset(a, off) \
	(abd_t *)((void *)(a)+(off))

#define	u_abd_put_offset(a) \
	do { } while (0)

#ifdef _KERNEL
#define	u_abd_copy_to_user_off(a, b, n, off) \
	copy_to_user(a, (void *)(b)+(off), n)

#define	u_abd_copy_from_user_off(a, b, n, off) \
	copy_from_user((void *)(a)+(off), b, n)

#define	u_abd_uiomove_off(p, n, rw, uio, off) \
	uiomove((void *)(p)+(off), n, rw, uio)

#define	u_abd_uiocopy_off(p, n, rw, uio, c, off) \
	uiocopy((void *)(p)+(off), n, rw, uio, c)

#define	u_abd_bio_map_off(bio, a, n, off) \
	bio_map(bio, (void *)(a)+(off), n)

#define	u_abd_bio_nr_pages_off(a, n, off) \
	bio_nr_pages((void *)(a)+(off), n)
#endif /* _KERNEL */

#define	u_abd_borrow_buf(a, n) \
	((void *)a)

#define	u_abd_borrow_buf_copy(a, n) \
	((void *)a)

#define	u_abd_return_buf(a, b, n) \
	do { } while (0)

#define	u_abd_return_buf_copy(a, b, n) \
	do { } while (0)


/*
 * Public ABD function wrappers
 */
/*
 * Iterate over ABD and call a read function @func.
 * @func should be implemented so that its behaviour is the same when taking
 * linear and when taking scatter
 */
#define	abd_iterate_rfunc(abd, size, func, priv) \
	ABD_FUNC_WRAPPER(abd, abd_iterate_rfunc, abd, size, func, priv)

/*
 * Iterate over ABD and call a write function @func.
 * @func should be implemented so that its behaviour is the same when taking
 * linear and when taking scatter
 */
#define	abd_iterate_wfunc(abd, size, func, priv) \
	ABD_FUNC_WRAPPER(abd, abd_iterate_wfunc, abd, size, func, priv)

/*
 * Iterate over two ABD and call @func2.
 * @func2 should be implemented so that its behaviour is the same when taking
 * linear and when taking scatter
 * @dabd and @sabd must be the same type
 */
#define	abd_iterate_func2(dabd, sabd, dsize, ssize, func2, priv) \
	ABD_FUNC2_WRAPPER(dabd, sabd, abd_iterate_func2, dabd, sabd, dsize, \
	    ssize, func2, priv)

/*
 * Copy between two ABD
 * Must be the same type
 */
#define	abd_copy(dabd, sabd, size) \
	ABD_FUNC2_WRAPPER(dabd, sabd, abd_copy, dabd, sabd, size)

/*
 * Copy from buffer to ABD
 * @off is the offset in @abd
 */
#define	abd_copy_from_buf_off(abd, buf, size, off) \
	ABD_FUNC_WRAPPER(abd, abd_copy_from_buf_off, abd, buf, size, off)

#define	abd_copy_from_buf(abd, buf, size) \
	abd_copy_from_buf_off(abd, buf, size, 0)

/*
 * Copy from ABD to buffer
 */
#define	abd_copy_to_buf_off(buf, abd, size, off) \
	ABD_FUNC_WRAPPER(abd, abd_copy_to_buf_off, buf, abd, size, off)

#define	abd_copy_to_buf(buf, abd, size) \
	abd_copy_to_buf_off(buf, abd, size, 0)

/*
 * Compare between two ABD.
 * Must be the same type
 */
#define	abd_cmp(dabd, sabd, size) \
	ABD_RET_FUNC2_WRAPPER(int, dabd, sabd, abd_cmp, dabd, sabd, size)

/*
 * Compare between buffer and ABD.
 */
#define	abd_cmp_buf_off(abd, buf, size, off) \
	ABD_RET_FUNC_WRAPPER(int, abd, abd_cmp_buf_off, abd, buf, size, off)

#define	abd_cmp_buf(abd, buf, size) \
	abd_cmp_buf_off(abd, buf, size, 0)

/*
 * Zero out an ABD.
 */
#define	abd_zero_off(abd, size, off) \
	ABD_FUNC_WRAPPER(abd, abd_zero_off, abd, size, off)

#define	abd_zero(abd, size) \
	abd_zero_off(abd, size, 0)

#ifdef _KERNEL
/*
 * Copy from ABD to user buffer.
 */
#define	abd_copy_to_user_off(buf, abd, size, off) \
	ABD_RET_FUNC_WRAPPER(int, abd, abd_copy_to_user_off, buf, abd, size, \
	    off)

#define	abd_copy_to_user(buf, abd, size) \
	abd_copy_to_user_off(buf, abd, size, 0)

/*
 * Copy from user buffer to ABD.
 */
#define	abd_copy_from_user_off(abd, buf, size, off) \
	ABD_RET_FUNC_WRAPPER(int, abd, abd_copy_from_user_off, abd, buf, size,\
	    off)

#define	abd_copy_from_user(abd, buf, size) \
	abd_copy_from_user_off(abd, buf, size, 0)

/*
 * uiomove for ABD.
 */
#define	abd_uiomove_off(abd, n, rw, uio, off) \
	ABD_RET_FUNC_WRAPPER(int, abd, abd_uiomove_off, abd, n, rw, uio, off)

#define	abd_uiomove(abd, n, rw, uio) \
	abd_uiomove_off(abd, n, rw, uio, 0)

/*
 * uiocopy for ABD.
 */
#define	abd_uiocopy_off(abd, n, rw, uio, c, off) \
	ABD_RET_FUNC_WRAPPER(int, abd, abd_uiocopy_off, abd, n, rw, uio, c, off)

#define	abd_uiocopy(abd, n, rw, uio, c) \
	abd_uiocopy_off(abd, n, rw, uio, c, 0)

/*
 * bio_map for ABD.
 */
#define	abd_bio_map_off(bio, abd, bio_size, off) \
	ABD_RET_FUNC_WRAPPER(unsigned int, abd, abd_bio_map_off, bio, abd, \
	    bio_size, off)

/*
 * bio_nr_pages for ABD.
 */
#define	abd_bio_nr_pages_off(abd, bio_size, off) \
	ABD_RET_FUNC_WRAPPER(unsigned long, abd, abd_bio_nr_pages_off, abd, \
	    bio_size, off)
#endif	/* _KERNEL */

/*
 * Allocate a new ABD to point to offset @off of the original ABD.
 * It shares the underlying buffer with the original ABD.
 * Use abd_put_offset to free. The original ABD(allocated from abd_alloc) must
 * not be freed before any of its derived ABD.
 */
#define	abd_get_offset(abd, off) \
	ABD_RET_FUNC_WRAPPER(abd_t *, abd, abd_get_offset, abd, off)

/*
 * Free an ABD allocated from abd_get_offset.
 * Must not be used on ABD from elsewhere.
 */
#define	abd_put_offset(abd) \
	ABD_FUNC_WRAPPER(abd, abd_put_offset, abd)

/*
 * Borrow a linear buffer for an ABD
 * Will allocate if ABD is scatter
 */
#define	abd_borrow_buf(abd, size) \
	ABD_RET_FUNC_WRAPPER(void *, abd, abd_borrow_buf, abd, size)

/*
 * Borrow a linear buffer for an ABD
 * Will allocate and copy if ABD is scatter
 */
#define	abd_borrow_buf_copy(abd, size) \
	ABD_RET_FUNC_WRAPPER(void *, abd, abd_borrow_buf_copy, abd, size)

/*
 * Return the linear buffer
 */
#define	abd_return_buf(abd, buf, size) \
	ABD_FUNC_WRAPPER(abd, abd_return_buf, abd, buf, size)

/*
 * Copy back to ABD and return the linear buffer
 */
#define	abd_return_buf_copy(abd, buf, size) \
	ABD_FUNC_WRAPPER(abd, abd_return_buf_copy, abd, buf, size)

#ifdef __cplusplus
}
#endif

#endif	/* _ABD_H */
