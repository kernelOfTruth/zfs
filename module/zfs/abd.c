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

#include <sys/abd.h>
#ifdef _KERNEL
#include <linux/kernel.h>
#include <linux/sched.h>
#include <linux/slab.h>
#include <linux/scatterlist.h>
#include <linux/gfp.h>
#include <linux/pagemap.h>
#include <linux/kmap_compat.h>

#else	/* _KERNEL */

/*
 * Userspace compatibility layer
 */

/*
 * page
 */
#ifndef PAGE_SIZE
#define	PAGE_SIZE 4096
#endif

struct page;

#define	alloc_page(gfp) \
	((struct page *)umem_alloc_aligned(PAGE_SIZE, PAGE_SIZE, UMEM_DEFAULT))

#define	__free_page(page) \
	umem_free(page, PAGE_SIZE)

/*
 * scatterlist
 */
struct scatterlist {
	struct page *page;
	int length;
	int end;
};

static void
sg_init_table(struct scatterlist *sg, int nr) {
	memset(sg, 0, nr * sizeof (struct scatterlist));
	sg[nr - 1].end = 1;
}

static inline void
sg_set_page(struct scatterlist *sg, struct page *page, unsigned int len,
    unsigned int offset) {
	/* currently we don't use offset */
	ASSERT(offset == 0);
	sg->page = page;
	sg->length = len;
}

static inline struct page *
sg_page(struct scatterlist *sg) {
	return (sg->page);
}

/*
 * sg_mapping_iter
 */
struct sg_mapping_iter {
	struct scatterlist *sg;
	int started;
	int nents;
	int length;
	void *addr;
};

void
__sg_miter_start(struct sg_mapping_iter *miter, struct scatterlist *sg,
    unsigned int nents) {
	memset(miter, 0, sizeof (struct sg_mapping_iter));
	miter->sg = sg;
	miter->nents = nents;
}

#define	sg_miter_start(miter, sg, nents, flags) \
	__sg_miter_start(miter, sg, nents)

#define	sg_miter_stop(miter) \
	do { } while (0)

int
sg_miter_next(struct sg_mapping_iter *miter) {
	if (!miter->nents)
		return (0);

	if (!miter->started)
		miter->started = 1;
	else if (miter->sg->end)
		return (0);
	else
		miter->sg++;

	miter->nents--;
	miter->length = miter->sg->length;
	miter->addr = (void *)miter->sg->page;
	return (1);
}

/*
 * misc
 */
#ifndef DIV_ROUND_UP
#define	DIV_ROUND_UP(n, d)		(((n) + (d) - 1) / (d))
#endif

#ifndef unlikely
#define	unlikely(x)			(x)
#endif

#define	kmap(page)			((void *)page)
#define	kunmap(page)			do { } while (0)
#define	zfs_kmap_atomic(page, type)	((void *)page)
#define	zfs_kunmap_atomic(addr, type)	do { } while (0)
#define	local_irq_save(flags)		do { flags = 0; } while (0)
#define	local_irq_restore(flags)	do { } while (0)
#define	flush_kernel_dcache_page(page)	do { } while (0)
#define	flush_dcache_page(page)		do { } while (0)
#define	set_current_state(state)	do { } while (0)
static inline long
schedule_timeout(long timeout)
{
	sleep(timeout);
	return (0);
}

#endif	/* _KERNEL */

#define	ABD_MITER_WFLAGS (SG_MITER_ATOMIC|SG_MITER_TO_SG)
#define	ABD_MITER_RFLAGS (SG_MITER_ATOMIC|SG_MITER_FROM_SG)

typedef struct arc_buf_data {
	uint64_t		abd_magic;
	size_t			abd_size;
	size_t			abd_offset;
	struct scatterlist	*abd_sgl;
	struct scatterlist	__abd_sgl[0];
} arc_buf_data_t;

#define	ABD_CHECK(__abd)						\
(									\
{									\
	arc_buf_data_t *___abd;						\
	ASSERT(ABD_IS_SCATTER(__abd));					\
	___abd = (arc_buf_data_t *)(((unsigned long)__abd) & ~0x1);	\
	ASSERT(___abd->abd_magic == ARC_BUF_DATA_MAGIC);		\
	___abd;								\
}									\
)

void
do_abd_iterate_rfunc(abd_t *__abd, size_t size,
    int (*func)(const void *, uint64_t, void *), void *private)
{
	size_t len;
	int stop, n;
	int skip_pn, skip_off;
	struct sg_mapping_iter miter;
	unsigned long flags;
	arc_buf_data_t *abd = ABD_CHECK(__abd);

	ASSERT(size <= abd->abd_size - abd->abd_offset);

	n = DIV_ROUND_UP(abd->abd_size, PAGE_SIZE);

	skip_pn = abd->abd_offset / PAGE_SIZE;
	skip_off = abd->abd_offset % PAGE_SIZE;

	sg_miter_start(&miter, &abd->abd_sgl[skip_pn], n - skip_pn,
	    ABD_MITER_RFLAGS);

	local_irq_save(flags);
	while (size > 0) {
		VERIFY(sg_miter_next(&miter));

		len = MIN(miter.length - skip_off, size);

		stop = func(miter.addr + skip_off, len, private);

		if (stop)
			break;
		size -= len;
		skip_off = 0;
	}
	sg_miter_stop(&miter);
	local_irq_restore(flags);
}

void
do_abd_iterate_wfunc(abd_t *__abd, size_t size,
    int (*func)(void *, uint64_t, void *), void *private)
{
	size_t len;
	int stop, n;
	int skip_pn, skip_off;
	struct sg_mapping_iter miter;
	unsigned long flags;
	arc_buf_data_t *abd = ABD_CHECK(__abd);

	ASSERT(size <= abd->abd_size - abd->abd_offset);

	n = DIV_ROUND_UP(abd->abd_size, PAGE_SIZE);

	skip_pn = abd->abd_offset / PAGE_SIZE;
	skip_off = abd->abd_offset % PAGE_SIZE;

	sg_miter_start(&miter, &abd->abd_sgl[skip_pn], n - skip_pn,
	    ABD_MITER_WFLAGS);

	local_irq_save(flags);
	while (size > 0) {
		VERIFY(sg_miter_next(&miter));

		len = MIN(miter.length - skip_off, size);

		stop = func(miter.addr + skip_off, len, private);

		if (stop)
			break;
		size -= len;
		skip_off = 0;
	}
	sg_miter_stop(&miter);
	local_irq_restore(flags);
}

void
do_abd_iterate_func2(abd_t *__dabd, abd_t *__sabd, size_t dsize, size_t ssize,
    int (*func2)(void *, void *, uint64_t, uint64_t, void *), void *private)
{
	size_t dlen, slen;
	int i, j, stop;
	int doff, soff;
	void *daddr = NULL, *saddr = NULL;
	arc_buf_data_t *dabd = ABD_CHECK(__dabd);
	arc_buf_data_t *sabd = ABD_CHECK(__sabd);

	ASSERT(dsize <= dabd->abd_size - dabd->abd_offset);
	ASSERT(ssize <= sabd->abd_size - sabd->abd_offset);

	i = dabd->abd_offset / PAGE_SIZE;
	doff = dabd->abd_offset % PAGE_SIZE;

	j = sabd->abd_offset / PAGE_SIZE;
	soff = sabd->abd_offset % PAGE_SIZE;

	while (dsize > 0 || ssize > 0) {
		dlen = MIN(PAGE_SIZE - doff, dsize);
		slen = MIN(PAGE_SIZE - soff, ssize);

		/* there are remainings after this run, use equal len */
		if (dsize > dlen || ssize > slen) {
			if (MIN(dlen, slen) > 0)
				slen = dlen = MIN(dlen, slen);
		}

		/* must be progressive */
		ASSERT(dlen > 0 || slen > 0);

		if (dlen)
			daddr = zfs_kmap_atomic(sg_page(&dabd->abd_sgl[i]),
			    KM_USER0);
		if (slen)
			saddr = zfs_kmap_atomic(sg_page(&sabd->abd_sgl[j]),
			    KM_USER1);

		stop = func2(daddr + doff, saddr + soff, dlen, slen, private);

		if (dlen) {
			flush_kernel_dcache_page(sg_page(&dabd->abd_sgl[i]));
			zfs_kunmap_atomic(saddr, KM_USER1);
		}
		if (slen) {
			flush_kernel_dcache_page(sg_page(&sabd->abd_sgl[j]));
			zfs_kunmap_atomic(daddr, KM_USER0);
		}
		if (stop)
			break;

		dsize -= dlen;
		ssize -= slen;
		doff += dlen;
		soff += slen;
		if (doff >= PAGE_SIZE) {
			i++;
			doff = 0;
		}
		if (soff >= PAGE_SIZE) {
			j++;
			soff = 0;
		}
	}
}

void
do_abd_copy(abd_t *__dabd, abd_t *__sabd, size_t size)
{
	size_t len;
	int i, j;
	int doff, soff;
	void *daddr, *saddr;
	arc_buf_data_t *dabd = ABD_CHECK(__dabd);
	arc_buf_data_t *sabd = ABD_CHECK(__sabd);

	ASSERT(size <= dabd->abd_size - dabd->abd_offset);
	ASSERT(size <= sabd->abd_size - sabd->abd_offset);

	i = dabd->abd_offset / PAGE_SIZE;
	doff = dabd->abd_offset % PAGE_SIZE;

	j = sabd->abd_offset / PAGE_SIZE;
	soff = sabd->abd_offset % PAGE_SIZE;

	while (size > 0) {
		len = MIN(PAGE_SIZE - doff, size);
		len = MIN(len, PAGE_SIZE - soff);

		daddr = zfs_kmap_atomic(sg_page(&dabd->abd_sgl[i]), KM_USER0);
		saddr = zfs_kmap_atomic(sg_page(&sabd->abd_sgl[j]), KM_USER1);

		memcpy(daddr + doff, saddr + soff, len);

		flush_kernel_dcache_page(sg_page(&dabd->abd_sgl[i]));
		zfs_kunmap_atomic(saddr, KM_USER1);
		zfs_kunmap_atomic(daddr, KM_USER0);
		size -= len;
		doff += len;
		soff += len;
		if (doff >= PAGE_SIZE) {
			i++;
			doff = 0;
		}
		if (soff >= PAGE_SIZE) {
			j++;
			soff = 0;
		}
	}
}

void
do_abd_copy_from_buf_off(abd_t *__abd, const void *buf, size_t size,
    size_t off)
{
	size_t len;
	int n;
	int skip_pn, skip_off;
	struct sg_mapping_iter miter;
	unsigned long flags;
	arc_buf_data_t *abd = ABD_CHECK(__abd);

	ASSERT(size <= abd->abd_size - abd->abd_offset - off);

	n = DIV_ROUND_UP(abd->abd_size, PAGE_SIZE);

	skip_pn = (abd->abd_offset + off) / PAGE_SIZE;
	skip_off = (abd->abd_offset + off) % PAGE_SIZE;

	sg_miter_start(&miter, &abd->abd_sgl[skip_pn], n - skip_pn,
	    ABD_MITER_WFLAGS);

	local_irq_save(flags);
	while (size > 0) {
		VERIFY(sg_miter_next(&miter));

		len = MIN(miter.length - skip_off, size);

		memcpy(miter.addr + skip_off, buf, len);

		size -= len;
		buf += len;
		skip_off = 0;
	}
	sg_miter_stop(&miter);
	local_irq_restore(flags);
}

void
do_abd_copy_to_buf_off(void *buf, abd_t *__abd, size_t size, size_t off)
{
	size_t len;
	int n;
	int skip_pn, skip_off;
	struct sg_mapping_iter miter;
	unsigned long flags;
	arc_buf_data_t *abd = ABD_CHECK(__abd);

	ASSERT(size <= abd->abd_size - abd->abd_offset - off);

	n = DIV_ROUND_UP(abd->abd_size, PAGE_SIZE);

	skip_pn = (abd->abd_offset + off) / PAGE_SIZE;
	skip_off = (abd->abd_offset + off) % PAGE_SIZE;

	sg_miter_start(&miter, &abd->abd_sgl[skip_pn], n - skip_pn,
	    ABD_MITER_RFLAGS);

	local_irq_save(flags);
	while (size > 0) {
		VERIFY(sg_miter_next(&miter));

		len = MIN(miter.length - skip_off, size);

		memcpy(buf, miter.addr + skip_off, len);

		size -= len;
		buf += len;
		skip_off = 0;
	}
	sg_miter_stop(&miter);
	local_irq_restore(flags);
}

int
do_abd_cmp(abd_t *__dabd, abd_t *__sabd, size_t size)
{
	size_t len;
	int i, j, ret = 0;
	int doff, soff;
	void *daddr, *saddr;
	arc_buf_data_t *dabd = ABD_CHECK(__dabd);
	arc_buf_data_t *sabd = ABD_CHECK(__sabd);

	ASSERT(size <= dabd->abd_size - dabd->abd_offset);
	ASSERT(size <= sabd->abd_size - sabd->abd_offset);

	i = dabd->abd_offset / PAGE_SIZE;
	doff = dabd->abd_offset % PAGE_SIZE;

	j = sabd->abd_offset / PAGE_SIZE;
	soff = sabd->abd_offset % PAGE_SIZE;

	while (size > 0) {
		len = MIN(PAGE_SIZE - doff, size);
		len = MIN(len, PAGE_SIZE - soff);

		daddr = zfs_kmap_atomic(sg_page(&dabd->abd_sgl[i]), KM_USER0);
		saddr = zfs_kmap_atomic(sg_page(&sabd->abd_sgl[j]), KM_USER1);

		ret = memcmp(daddr + doff, saddr + soff, len);

		zfs_kunmap_atomic(saddr, KM_USER1);
		zfs_kunmap_atomic(daddr, KM_USER0);

		if (ret)
			break;

		size -= len;
		doff += len;
		soff += len;
		if (doff >= PAGE_SIZE) {
			i++;
			doff = 0;
		}
		if (soff >= PAGE_SIZE) {
			j++;
			soff = 0;
		}
	}
	return (ret);
}

int
do_abd_cmp_buf_off(abd_t *__abd, const void *buf, size_t size, size_t off)
{
	size_t len;
	int n, ret = 0;
	int skip_pn, skip_off;
	struct sg_mapping_iter miter;
	unsigned long flags;
	arc_buf_data_t *abd = ABD_CHECK(__abd);

	ASSERT(size <= abd->abd_size - abd->abd_offset - off);

	n = DIV_ROUND_UP(abd->abd_size, PAGE_SIZE);

	skip_pn = (abd->abd_offset + off) / PAGE_SIZE;
	skip_off = (abd->abd_offset + off) % PAGE_SIZE;

	sg_miter_start(&miter, &abd->abd_sgl[skip_pn], n - skip_pn,
	    ABD_MITER_RFLAGS);

	local_irq_save(flags);
	while (size > 0) {
		VERIFY(sg_miter_next(&miter));

		len = MIN(miter.length - skip_off, size);

		ret = memcmp(miter.addr + skip_off, buf, len);

		if (ret)
			break;

		size -= len;
		buf += len;
		skip_off = 0;
	}
	sg_miter_stop(&miter);
	local_irq_restore(flags);
	return (ret);
}

void
do_abd_zero_off(abd_t *__abd, size_t size, size_t off)
{
	size_t len;
	int n;
	int skip_pn, skip_off;
	struct sg_mapping_iter miter;
	unsigned long flags;
	arc_buf_data_t *abd = ABD_CHECK(__abd);

	ASSERT(size <= abd->abd_size - abd->abd_offset - off);

	n = DIV_ROUND_UP(abd->abd_size, PAGE_SIZE);

	skip_pn = (abd->abd_offset + off) / PAGE_SIZE;
	skip_off = (abd->abd_offset + off) % PAGE_SIZE;

	sg_miter_start(&miter, &abd->abd_sgl[skip_pn], n - skip_pn,
	    ABD_MITER_WFLAGS);

	local_irq_save(flags);
	while (size > 0) {
		VERIFY(sg_miter_next(&miter));

		len = MIN(miter.length - skip_off, size);

		memset(miter.addr + skip_off, 0, len);

		size -= len;
		skip_off = 0;
	}
	sg_miter_stop(&miter);
	local_irq_restore(flags);
}

#ifdef _KERNEL
int
do_abd_copy_to_user_off(void __user *buf, abd_t *__abd, size_t size,
    size_t off)
{
	int i, ret = 0;
	size_t len;
	int skip_pn, skip_off;
	void *maddr;
	arc_buf_data_t *abd = ABD_CHECK(__abd);

	ASSERT(size <= abd->abd_size - abd->abd_offset - off);

	skip_pn = (abd->abd_offset + off) / PAGE_SIZE;
	skip_off = (abd->abd_offset + off) % PAGE_SIZE;

	for (i = skip_pn; size > 0; i++) {
		len = MIN(size, PAGE_SIZE - skip_off);

		maddr = zfs_kmap_atomic(sg_page(&abd->abd_sgl[i]), KM_USER0);
		ret = __copy_to_user_inatomic(buf, maddr + skip_off, len);
		zfs_kunmap_atomic(maddr, KM_USER0);
		if (ret) {
			maddr = kmap(sg_page(&abd->abd_sgl[i]));
			ret = copy_to_user(buf, maddr + skip_off, len);
			kunmap(sg_page(&abd->abd_sgl[i]));

			if (ret)
				break;
		}

		size -= len;
		buf += len;
		skip_off = 0;
	}
	return (ret ? EFAULT : 0);
}

int
do_abd_copy_from_user_off(abd_t *__abd, const void __user *buf, size_t size,
    size_t off)
{
	int i, ret = 0;
	size_t len;
	int skip_pn, skip_off;
	void *maddr;
	arc_buf_data_t *abd = ABD_CHECK(__abd);

	ASSERT(size <= abd->abd_size - abd->abd_offset - off);

	skip_pn = (abd->abd_offset + off) / PAGE_SIZE;
	skip_off = (abd->abd_offset + off) % PAGE_SIZE;

	for (i = skip_pn; size > 0; i++) {
		len = MIN(size, PAGE_SIZE - skip_off);

		maddr = zfs_kmap_atomic(sg_page(&abd->abd_sgl[i]), KM_USER0);
		ret = __copy_from_user_inatomic(maddr + skip_off, buf, len);
		flush_dcache_page(sg_page(&abd->abd_sgl[i]));
		zfs_kunmap_atomic(maddr, KM_USER0);
		if (ret) {
			maddr = kmap(sg_page(&abd->abd_sgl[i]));
			ret = copy_from_user(maddr + skip_off, buf, len);
			flush_dcache_page(sg_page(&abd->abd_sgl[i]));
			kunmap(sg_page(&abd->abd_sgl[i]));

			if (ret)
				break;
		}

		size -= len;
		buf += len;
		skip_off = 0;
	}
	return (ret ? EFAULT : 0);
}

int
do_abd_uiomove_off(abd_t *__abd, size_t n, enum uio_rw rw, uio_t *uio,
    size_t off)
{
	struct iovec *iov;
	ulong_t cnt;

	while (n && uio->uio_resid) {
		iov = uio->uio_iov;
		cnt = MIN(iov->iov_len, n);
		if (cnt == 0l) {
			uio->uio_iov++;
			uio->uio_iovcnt--;
			continue;
		}
		switch (uio->uio_segflg) {
		case UIO_USERSPACE:
		case UIO_USERISPACE:
			/*
			 * p = kernel data pointer
			 * iov->iov_base = user data pointer
			 */
			if (rw == UIO_READ) {
				if (do_abd_copy_to_user_off(iov->iov_base,
				    __abd, cnt, off))
					return (EFAULT);
			} else {
				if (do_abd_copy_from_user_off(__abd,
				    iov->iov_base, cnt, off))
					return (EFAULT);
			}
			break;
		case UIO_SYSSPACE:
			if (rw == UIO_READ)
				do_abd_copy_to_buf_off(iov->iov_base, __abd,
				    cnt, off);
			else
				do_abd_copy_from_buf_off(__abd, iov->iov_base,
				    cnt, off);
			break;
		}
		iov->iov_base += cnt;
		iov->iov_len -= cnt;
		uio->uio_resid -= cnt;
		uio->uio_loffset += cnt;
		off += cnt;
		n -= cnt;
	}
	return (0);
}

int
do_abd_uiocopy_off(abd_t *__abd, size_t n, enum uio_rw rw, uio_t *uio,
    size_t *cbytes, size_t off)
{
	struct iovec *iov;
	ulong_t cnt;
	int iovcnt;

	iovcnt = uio->uio_iovcnt;
	*cbytes = 0;

	for (iov = uio->uio_iov; n && iovcnt; iov++, iovcnt--) {
		cnt = MIN(iov->iov_len, n);
		if (cnt == 0)
			continue;

		switch (uio->uio_segflg) {

		case UIO_USERSPACE:
		case UIO_USERISPACE:
			/*
			 * p = kernel data pointer
			 * iov->iov_base = user data pointer
			 */
			if (rw == UIO_READ) {
				/* UIO_READ = copy data from kernel to user */
				if (do_abd_copy_to_user_off(iov->iov_base,
				    __abd, cnt, off))
					return (EFAULT);
			} else {
				/* UIO_WRITE = copy data from user to kernel */
				if (do_abd_copy_from_user_off(__abd,
				    iov->iov_base, cnt, off))
					return (EFAULT);
			}
			break;

		case UIO_SYSSPACE:
			if (rw == UIO_READ)
				do_abd_copy_to_buf_off(iov->iov_base, __abd,
				    cnt, off);
			else
				do_abd_copy_from_buf_off(__abd, iov->iov_base,
				    cnt, off);
			break;
		}
		off += cnt;
		n -= cnt;
		*cbytes += cnt;
	}
	return (0);
}

unsigned int
do_abd_bio_map_off(struct bio *bio, abd_t *__abd, unsigned int bio_size,
    size_t off)
{
	int i;
	size_t len;
	int skip_pn, skip_off;
	arc_buf_data_t *abd = ABD_CHECK(__abd);

	ASSERT(bio_size <= abd->abd_size - abd->abd_offset - off);

	skip_pn = (abd->abd_offset + off) / PAGE_SIZE;
	skip_off = (abd->abd_offset + off) % PAGE_SIZE;

	for (i = 0; i < bio->bi_max_vecs; i++) {
		if (bio_size <= 0)
			break;

		len = MIN(bio_size, PAGE_SIZE - skip_off);

		if (bio_add_page(bio, sg_page(&abd->abd_sgl[skip_pn+i]), len,
		    skip_off) != len)
			break;

		bio_size -= len;
		skip_off = 0;
	}
	return (bio_size);
}

unsigned long
do_abd_bio_nr_pages_off(abd_t *__abd, unsigned int bio_size, size_t off)
{
	arc_buf_data_t *abd = ABD_CHECK(__abd);
	return ((abd->abd_offset + off + bio_size + PAGE_SIZE-1)>>PAGE_SHIFT) -
	    ((abd->abd_offset + off)>>PAGE_SHIFT);
}
#endif	/* _KERNEL */

abd_t *
do_abd_get_offset(abd_t *__sabd, size_t off)
{
	arc_buf_data_t *abd;
	arc_buf_data_t *sabd = ABD_CHECK(__sabd);

	abd = kmem_alloc(sizeof (arc_buf_data_t), KM_PUSHPAGE);

	ASSERT(abd);
	ASSERT_ABD_LINEAR(abd);

	abd->abd_magic = ARC_BUF_DATA_MAGIC;
	abd->abd_size = sabd->abd_size;
	abd->abd_offset = sabd->abd_offset + off;
	abd->abd_sgl = sabd->abd_sgl;

	return (abd_t *)(((unsigned long)abd)|0x1);
}

void
do_abd_put_offset(abd_t *__abd)
{
	arc_buf_data_t *abd = ABD_CHECK(__abd);

	ASSERT(abd->abd_sgl != &abd->__abd_sgl[0]);

	abd->abd_magic = 0;
	kmem_free(abd, sizeof (arc_buf_data_t));
}

abd_t *
abd_alloc(size_t size)
{
	arc_buf_data_t *abd;
	struct page *page;
	int i, n = DIV_ROUND_UP(size, PAGE_SIZE);

	abd = kmem_alloc(sizeof (arc_buf_data_t) +
	    n * sizeof (struct scatterlist), KM_PUSHPAGE);

	ASSERT(abd);
	ASSERT_ABD_LINEAR(abd);

	abd->abd_magic = ARC_BUF_DATA_MAGIC;
	abd->abd_size = size;
	abd->abd_offset = 0;
	abd->abd_sgl = &abd->__abd_sgl[0];
	sg_init_table(abd->abd_sgl, n);

	for (i = 0; i < n; i++) {
retry:
		page = alloc_page(GFP_NOIO|__GFP_HIGHMEM);
		if (unlikely(page == NULL)) {
			set_current_state(TASK_INTERRUPTIBLE);
			schedule_timeout(1);
			goto retry;
		}
		sg_set_page(&abd->abd_sgl[i], page, PAGE_SIZE, 0);
	}

	return (abd_t *)(((unsigned long)abd)|0x1);
}

void
abd_free(abd_t *__abd, size_t size)
{
	int i, n;
	struct page *page;
	arc_buf_data_t *abd = ABD_CHECK(__abd);

	ASSERT(abd->abd_sgl == &abd->__abd_sgl[0]);
	ASSERT(abd->abd_size == size);
	n =  DIV_ROUND_UP(abd->abd_size, PAGE_SIZE);

	abd->abd_magic = 0;
	for (i = 0; i < n; i++) {
		page = sg_page(&abd->abd_sgl[i]);
		if (page)
			__free_page(page);
	}
	kmem_free(abd, sizeof (arc_buf_data_t) +
	    n * sizeof (struct scatterlist));
}
