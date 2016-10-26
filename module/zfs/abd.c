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
 * Copyright (c) 2016 by Delphix. All rights reserved.
 */

/*
 * ARC buffer data (ABD).
 *
 * ABDs are an abstract data structure for the ARC which can use two
 * different ways of storing the underlying data:
 *
 * (a) Linear buffer. In this case, all the data in the ABD is stored in one
 *     contiguous buffer in memory (from a zio_[data_]buf_* kmem cache).
 *
 *         +-------------------+
 *         | ABD (linear)      |
 *         |   abd_flags = ... |
 *         |   abd_size = ...  |     +--------------------------------+
 *         |   abd_buf ------------->| raw buffer of size abd_size    |
 *         +-------------------+     +--------------------------------+
 *              no abd_chunks
 *
 * (b) Scattered buffer. In this case, the data in the ABD is split into
 *     equal-sized chunks (from the abd_chunk_cache kmem_cache), with pointers
 *     to the chunks recorded in an array at the end of the ABD structure.
 *
 *         +-------------------+
 *         | ABD (scattered)   |
 *         |   abd_flags = ... |
 *         |   abd_size = ...  |
 *         |   abd_offset = 0  |                           +-----------+
 *         |   abd_chunks[0] ----------------------------->| chunk 0   |
 *         |   abd_chunks[1] ---------------------+        +-----------+
 *         |   ...             |                  |        +-----------+
 *         |   abd_chunks[N-1] ---------+         +------->| chunk 1   |
 *         +-------------------+        |                  +-----------+
 *                                      |                      ...
 *                                      |                  +-----------+
 *                                      +----------------->| chunk N-1 |
 *                                                         +-----------+
 *
 * Linear buffers act exactly like normal buffers and are always mapped into the
 * kernel's virtual memory space, while scattered ABD data chunks are allocated
 * as physical pages and then mapped in only while they are actually being
 * accessed through one of the abd_* library functions. Using scattered ABDs
 * provides several benefits:
 *
 *  (1) They avoid use of kmem_*, preventing performance problems where running
 *      kmem_reap on very large memory systems never finishes and causes
 *      constant TLB shootdowns.
 *
 *  (2) Fragmentation is less of an issue since when we are at the limit of
 *      allocatable space, we won't have to search around for a long free
 *      hole in the VA space for large ARC allocations. Each chunk is mapped in
 *      individually, so even if we weren't using segkpm (see next point) we
 *      wouldn't need to worry about finding a contiguous address range.
 *
 *  (3) Use of segkpm will avoid the need for map / unmap / TLB shootdown costs
 *      on each ABD access. (If segkpm isn't available then we use all linear
 *      ABDs to avoid this penalty.) See seg_kpm.c for more details.
 *
 * It is possible to make all ABDs linear by setting zfs_abd_scatter_enabled to
 * B_FALSE. However, it is not possible to use scattered ABDs if segkpm is not
 * available, which is the case on all 32-bit systems and any 64-bit systems
 * where kpm_enable is turned off.
 *
 * In addition to directly allocating a linear or scattered ABD, it is also
 * possible to create an ABD by requesting the "sub-ABD" starting at an offset
 * within an existing ABD. In linear buffers this is simple (set abd_buf of
 * the new ABD to the starting point within the original raw buffer), but
 * scattered ABDs are a little more complex. The new ABD makes a copy of the
 * relevant abd_chunks pointers (but not the underlying data). However, to
 * provide arbitrary rather than only chunk-aligned starting offsets, it also
 * tracks an abd_offset field which represents the starting point of the data
 * within the first chunk in abd_chunks. For both linear and scattered ABDs,
 * creating an offset ABD marks the original ABD as the offset's parent, and the
 * original ABD's abd_children refcount is incremented. This data allows us to
 * ensure the root ABD isn't deleted before its children.
 *
 * Most consumers should never need to know what type of ABD they're using --
 * the ABD public API ensures that it's possible to transparently switch from
 * using a linear ABD to a scattered one when doing so would be beneficial.
 *
 * If you need to use the data within an ABD directly, if you know it's linear
 * (because you allocated it) you can use abd_to_buf() to access the underlying
 * raw buffer. Otherwise, you should use one of the abd_borrow_buf* functions
 * which will allocate a raw buffer if necessary. Use the abd_return_buf*
 * functions to return any raw buffers that are no longer necessary when you're
 * done using them.
 *
 * There are a variety of ABD APIs that implement basic buffer operations:
 * compare, copy, read, write, and fill with zeroes. If you need a custom
 * function which progressively accesses the whole ABD, use the abd_iterate_*
 * functions.
 */

#include <sys/abd.h>
#include <sys/param.h>
#include <sys/zio.h>
#include <sys/zfs_context.h>
#include <sys/zfs_znode.h>
#ifdef _KERNEL
#include <linux/scatterlist.h>
#include <linux/kmap_compat.h>
#endif

#ifndef KMC_NOTOUCH
#define	KMC_NOTOUCH	0
#endif

typedef struct abd_stats {
	kstat_named_t abdstat_struct_size;
	kstat_named_t abdstat_scatter_cnt;
	kstat_named_t abdstat_scatter_data_size;
	kstat_named_t abdstat_scatter_chunk_waste;
	kstat_named_t abdstat_linear_cnt;
	kstat_named_t abdstat_linear_data_size;
} abd_stats_t;

static abd_stats_t abd_stats = {
	/* Amount of memory occupied by all of the abd_t struct allocations */
	{ "struct_size",			KSTAT_DATA_UINT64 },
	/*
	 * The number of scatter ABDs which are currently allocated, excluding
	 * ABDs which don't own their data (for instance the ones which were
	 * allocated through abd_get_offset()).
	 */
	{ "scatter_cnt",			KSTAT_DATA_UINT64 },
	/* Amount of data stored in all scatter ABDs tracked by scatter_cnt */
	{ "scatter_data_size",			KSTAT_DATA_UINT64 },
	/*
	 * The amount of space wasted at the end of the last chunk across all
	 * scatter ABDs tracked by scatter_cnt.
	 */
	{ "scatter_chunk_waste",		KSTAT_DATA_UINT64 },
	/*
	 * The number of linear ABDs which are currently allocated, excluding
	 * ABDs which don't own their data (for instance the ones which were
	 * allocated through abd_get_offset() and abd_get_from_buf()). If an
	 * ABD takes ownership of its buf then it will become tracked.
	 */
	{ "linear_cnt",				KSTAT_DATA_UINT64 },
	/* Amount of data stored in all linear ABDs tracked by linear_cnt */
	{ "linear_data_size",			KSTAT_DATA_UINT64 },
};

#define	ABDSTAT(stat)		(abd_stats.stat.value.ui64)
#define	ABDSTAT_INCR(stat, val) \
	atomic_add_64(&abd_stats.stat.value.ui64, (val))
#define	ABDSTAT_BUMP(stat)	ABDSTAT_INCR(stat, 1)
#define	ABDSTAT_BUMPDOWN(stat)	ABDSTAT_INCR(stat, -1)

/* see block comment above for description */
int zfs_abd_scatter_enabled = B_TRUE;


#ifdef _KERNEL
static kstat_t *abd_ksp;

static struct page *
abd_alloc_chunk(void)
{
	struct page *c;
	gfp_t gfp = kmem_flags_convert(KM_SLEEP);
#ifdef CONFIG_HIGHMEM
	gfp |= __GFP_HIGHMEM;
#endif
	while (!(c = alloc_page(gfp)))
		schedule_timeout_interruptible(1);
	ASSERT3P(c, !=, NULL);
	return (c);
}

static void
abd_free_chunk(struct page *c)
{
	__free_page(c);
}

void
abd_init(void)
{
	abd_ksp = kstat_create("zfs", 0, "abdstats", "misc", KSTAT_TYPE_NAMED,
	    sizeof (abd_stats) / sizeof (kstat_named_t), KSTAT_FLAG_VIRTUAL);
	if (abd_ksp != NULL) {
		abd_ksp->ks_data = &abd_stats;
		kstat_install(abd_ksp);
	}
}

void
abd_fini(void)
{
	if (abd_ksp != NULL) {
		kstat_delete(abd_ksp);
		abd_ksp = NULL;
	}
}

#else

#ifndef PAGE_SHIFT
#define	PAGE_SHIFT (highbit64(PAGESIZE)-1)
#endif
struct page;
#define	kpm_enable			1
#define	abd_alloc_chunk() \
	((struct page *) umem_alloc_aligned(PAGESIZE, 64, KM_SLEEP))
#define	abd_free_chunk(chunk)		umem_free(chunk, PAGESIZE)
#define	zfs_kmap_atomic(chunk, km)	((void *)chunk)
#define	zfs_kunmap_atomic(addr, km)	do { (void)(addr); } while (0)
#define	local_irq_save(flags)		do { (void)(flags); } while (0)
#define	local_irq_restore(flags)	do { (void)(flags); } while (0)
#define	nth_page(pg, i) \
	((struct page *)((void *)(pg) + (i) * PAGESIZE))

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

#define	for_each_sg(sgl, sg, nr, i)	\
	for ((i) = 0, (sg) = (sgl); (i) < (nr); (i)++, (sg) = sg_next(sg))

static inline void
sg_set_page(struct scatterlist *sg, struct page *page, unsigned int len,
    unsigned int offset)
{
	/* currently we don't use offset */
	ASSERT(offset == 0);
	sg->page = page;
	sg->length = len;
}

static inline struct page *
sg_page(struct scatterlist *sg)
{
	return (sg->page);
}

static inline struct scatterlist *
sg_next(struct scatterlist *sg)
{
	if (sg->end)
		return (NULL);
	return (sg + 1);
}

void
abd_init(void)
{
}

void
abd_fini(void)
{
}

#endif /* _KERNEL */

#define	ABD_SCATTER(abd)	(abd->abd_u.abd_scatter)
#define	ABD_BUF(abd)		(abd->abd_u.abd_linear.abd_buf)
#define	abd_for_each_sg(abd, sg, n, i)	\
	for_each_sg(ABD_SCATTER(abd).abd_sgl, sg, n, i)

static inline size_t
abd_chunkcnt_for_bytes(size_t size)
{
	return (P2ROUNDUP(size, PAGESIZE) / PAGESIZE);
}

static inline void
abd_verify(abd_t *abd)
{
	ASSERT3U(abd->abd_size, >, 0);
	ASSERT3U(abd->abd_size, <=, SPA_MAXBLOCKSIZE);
	ASSERT3U(abd->abd_flags, ==, abd->abd_flags & (ABD_FLAG_LINEAR |
	    ABD_FLAG_OWNER | ABD_FLAG_META));
	IMPLY(abd->abd_parent != NULL, !(abd->abd_flags & ABD_FLAG_OWNER));
	IMPLY(abd->abd_flags & ABD_FLAG_META, abd->abd_flags & ABD_FLAG_OWNER);
	if (abd_is_linear(abd)) {
		ASSERT3P(abd->abd_u.abd_linear.abd_buf, !=, NULL);
	} else {
		size_t n;
		int i;
		struct scatterlist *sg;

		ASSERT3U(ABD_SCATTER(abd).abd_nents, >, 0);
		ASSERT3U(ABD_SCATTER(abd).abd_offset, <,
		    ABD_SCATTER(abd).abd_sgl->length);
		n = ABD_SCATTER(abd).abd_nents;
		abd_for_each_sg(abd, sg, n, i) {
			ASSERT3P(sg_page(sg), !=, NULL);
		}
	}
}

static inline abd_t *
abd_alloc_struct(void)
{
	size_t size = sizeof (abd_t);
	abd_t *abd = kmem_alloc(size, KM_PUSHPAGE);

	ASSERT3P(abd, !=, NULL);
	ABDSTAT_INCR(abdstat_struct_size, size);

	return (abd);
}

static inline void
abd_free_struct(abd_t *abd)
{
	int size = sizeof (abd_t);

	kmem_free(abd, size);
	ABDSTAT_INCR(abdstat_struct_size, -size);
}

#define	MAX_ALLOC_SIZE (1024*1024)

static int
abd_alloc_pages_merge(abd_t *abd, size_t size)
{
#if defined(_KERNEL) && !defined(CONFIG_HIGHMEM) && defined(HAVE_SG_FROM_PAGES)
	struct sg_table table;
	struct page **pages;
	unsigned long paddr;
	size_t s;
	int i, ret, n = abd_chunkcnt_for_bytes(size);
	gfp_t gfp = kmem_flags_convert(KM_SLEEP);

	pages = kmem_alloc(sizeof (*pages) * n, KM_NOSLEEP);
	if (!pages)
		return (-1);

	i = 0;
	s = size;
	while (s > 0) {
		ssize_t len = MIN(s, MAX_ALLOC_SIZE);
		paddr = (unsigned long)alloc_pages_exact(len,
		    GFP_NOWAIT|__GFP_NOWARN);
		if (paddr == 0)
			break;
		s -= len;
		for (; len > 0; len -= PAGESIZE, paddr += PAGESIZE, i++)
			pages[i] = virt_to_page(paddr);
	}

	for (; i < n; i++)
		while (!(pages[i] = alloc_page(gfp)))
			schedule_timeout_interruptible(1);

	while ((ret = sg_alloc_table_from_pages(&table, pages, n, 0,
	    size, gfp))) {
		VERIFY3S(ret, ==, -ENOMEM);
		schedule_timeout_interruptible(1);
	}

	ABD_SCATTER(abd).abd_sgl = table.sgl;
	ABD_SCATTER(abd).abd_nents = table.nents;

	kmem_free(pages, sizeof (*pages) * n);
	return (0);
#else
	return (-1);
#endif
}

static void
abd_alloc_pages_nomerge(abd_t *abd, size_t size)
{
	int i, n = abd_chunkcnt_for_bytes(size);
	struct scatterlist *sg;
#ifdef _KERNEL
	struct sg_table table;
	int ret;
	gfp_t gfp = kmem_flags_convert(KM_SLEEP);

	while ((ret = sg_alloc_table(&table, n, gfp))) {
		VERIFY3S(ret, ==, -ENOMEM);
		schedule_timeout_interruptible(1);
	}

	ASSERT3U(table.nents, ==, n);
	ABD_SCATTER(abd).abd_sgl = table.sgl;
#else
	ABD_SCATTER(abd).abd_sgl = vmem_alloc(n * sizeof (struct scatterlist),
	    KM_SLEEP);
	sg_init_table(ABD_SCATTER(abd).abd_sgl, n);
#endif
	ABD_SCATTER(abd).abd_nents = n;

	abd_for_each_sg(abd, sg, n, i) {
		struct page *p = abd_alloc_chunk();
		sg_set_page(sg, p, PAGESIZE, 0);
	}
}

/*
 * Allocate an ABD, along with its own underlying data buffers. Use this if you
 * don't care whether the ABD is linear or not.
 */
abd_t *
abd_alloc(size_t size, boolean_t is_metadata)
{
	abd_t *abd;

	if (!zfs_abd_scatter_enabled || size < PAGESIZE)
		return (abd_alloc_linear(size, is_metadata));

	VERIFY3U(size, <=, SPA_MAXBLOCKSIZE);

	abd = abd_alloc_struct();
	if (abd_alloc_pages_merge(abd, size) < 0)
		abd_alloc_pages_nomerge(abd, size);

	abd->abd_flags = ABD_FLAG_OWNER;
	if (is_metadata) {
		abd->abd_flags |= ABD_FLAG_META;
	}
	abd->abd_size = size;
	abd->abd_parent = NULL;
	refcount_create(&abd->abd_children);

	abd->abd_u.abd_scatter.abd_offset = 0;

	ABDSTAT_BUMP(abdstat_scatter_cnt);
	ABDSTAT_INCR(abdstat_scatter_data_size, size);
	ABDSTAT_INCR(abdstat_scatter_chunk_waste,
	    P2ROUNDUP(size, PAGESIZE) - size);

	return (abd);
}

static void
abd_free_pages(abd_t *abd)
{
	int i, j, n = ABD_SCATTER(abd).abd_nents;
	struct scatterlist *sg;
#ifdef _KERNEL
	struct sg_table table;
#endif
	abd_for_each_sg(abd, sg, n, i) {
		for (j = 0; j < sg->length; j += PAGESIZE) {
			struct page *p = nth_page(sg_page(sg), j>>PAGE_SHIFT);
			abd_free_chunk(p);
		}
	}
#ifdef _KERNEL
	table.sgl = ABD_SCATTER(abd).abd_sgl;
	table.nents = table.orig_nents = n;
	sg_free_table(&table);
#else
	vmem_free(ABD_SCATTER(abd).abd_sgl, n * sizeof (struct scatterlist));
#endif
}

static void
abd_free_scatter(abd_t *abd)
{
	abd_free_pages(abd);

	refcount_destroy(&abd->abd_children);
	ABDSTAT_BUMPDOWN(abdstat_scatter_cnt);
	ABDSTAT_INCR(abdstat_scatter_data_size, -(int)abd->abd_size);
	ABDSTAT_INCR(abdstat_scatter_chunk_waste,
	    abd->abd_size - P2ROUNDUP(abd->abd_size, PAGESIZE));

	abd_free_struct(abd);
}

/*
 * Allocate an ABD that must be linear, along with its own underlying data
 * buffer. Only use this when it would be very annoying to write your ABD
 * consumer with a scattered ABD.
 */
abd_t *
abd_alloc_linear(size_t size, boolean_t is_metadata)
{
	abd_t *abd = abd_alloc_struct();

	VERIFY3U(size, <=, SPA_MAXBLOCKSIZE);

	abd->abd_flags = ABD_FLAG_LINEAR | ABD_FLAG_OWNER;
	if (is_metadata) {
		abd->abd_flags |= ABD_FLAG_META;
	}
	abd->abd_size = size;
	abd->abd_parent = NULL;
	refcount_create(&abd->abd_children);

	if (is_metadata) {
		abd->abd_u.abd_linear.abd_buf = zio_buf_alloc(size);
	} else {
		abd->abd_u.abd_linear.abd_buf = zio_data_buf_alloc(size);
	}

	ABDSTAT_BUMP(abdstat_linear_cnt);
	ABDSTAT_INCR(abdstat_linear_data_size, size);

	return (abd);
}

static void
abd_free_linear(abd_t *abd)
{
	if (abd->abd_flags & ABD_FLAG_META) {
		zio_buf_free(abd->abd_u.abd_linear.abd_buf, abd->abd_size);
	} else {
		zio_data_buf_free(abd->abd_u.abd_linear.abd_buf, abd->abd_size);
	}

	refcount_destroy(&abd->abd_children);
	ABDSTAT_BUMPDOWN(abdstat_linear_cnt);
	ABDSTAT_INCR(abdstat_linear_data_size, -(int)abd->abd_size);

	abd_free_struct(abd);
}

/*
 * Free an ABD. Only use this on ABDs allocated with abd_alloc() or
 * abd_alloc_linear().
 */
void
abd_free(abd_t *abd)
{
	abd_verify(abd);
	ASSERT3P(abd->abd_parent, ==, NULL);
	ASSERT(abd->abd_flags & ABD_FLAG_OWNER);
	if (abd_is_linear(abd))
		abd_free_linear(abd);
	else
		abd_free_scatter(abd);
}

/*
 * Allocate an ABD of the same format (same metadata flag, same scatterize
 * setting) as another ABD.
 */
abd_t *
abd_alloc_sametype(abd_t *sabd, size_t size)
{
	boolean_t is_metadata = (sabd->abd_flags | ABD_FLAG_META) != 0;
	if (abd_is_linear(sabd)) {
		return (abd_alloc_linear(size, is_metadata));
	} else {
		return (abd_alloc(size, is_metadata));
	}
}

/*
 * If we're going to use this ABD for doing I/O using the block layer, the
 * consumer of the ABD data doesn't care if it's scattered or not, and we don't
 * plan to store this ABD in memory for a long period of time, we should
 * allocate the ABD type that requires the least data copying to do the I/O.
 *
 * Currently this is linear ABDs, however if ldi_strategy() can ever issue I/Os
 * using a scatter/gather list we should switch to that and replace this call
 * with vanilla abd_alloc().
 *
 * LINUX ABD TODO - once vdev_disk.c has ABD page support change to vanilla
 * - TODO vdev_disk.c now has ABD page support, but some disk label checksumming
 *   code still assumes linear ABD.
 */
abd_t *
abd_alloc_for_io(size_t size, boolean_t is_metadata)
{
	return (abd_alloc_linear(size, is_metadata));
}

abd_t *
abd_alloc_for_io_nosleep(size_t size, boolean_t is_metadata)
{
	/*
	 * ABD TODO -- Need to refactor to allow for nosleep option
	 */
	return (abd_alloc_linear(size, is_metadata));
}


/*
 * Allocate a new ABD to point to offset off of sabd. It shares the underlying
 * buffer data with sabd. Use abd_put() to free. sabd must not be freed while
 * any derived ABDs exist.
 */
static inline abd_t *
abd_get_offset_impl(abd_t *sabd, size_t off, size_t size)
{
	abd_t *abd;

	abd_verify(sabd);
	ASSERT3U(off, <=, sabd->abd_size);

	if (abd_is_linear(sabd)) {
		abd = abd_alloc_struct();

		/*
		 * Even if this buf is filesystem metadata, we only track that
		 * if we own the underlying data buffer, which is not true in
		 * this case. Therefore, we don't ever use ABD_FLAG_META here.
		 */
		abd->abd_flags = ABD_FLAG_LINEAR;

		abd->abd_u.abd_linear.abd_buf =
		    (char *)sabd->abd_u.abd_linear.abd_buf + off;
	} else {
		int i;
		struct scatterlist *sg;
		size_t new_offset = sabd->abd_u.abd_scatter.abd_offset + off;

		abd = abd_alloc_struct();

		/*
		 * Even if this buf is filesystem metadata, we only track that
		 * if we own the underlying data buffer, which is not true in
		 * this case. Therefore, we don't ever use ABD_FLAG_META here.
		 */
		abd->abd_flags = 0;

		abd_for_each_sg(sabd, sg, ABD_SCATTER(sabd).abd_nents, i) {
			if (new_offset < sg->length)
				break;
			new_offset -= sg->length;
		}

		ABD_SCATTER(abd).abd_sgl = sg;
		ABD_SCATTER(abd).abd_offset = new_offset;
		ABD_SCATTER(abd).abd_nents = ABD_SCATTER(sabd).abd_nents - i;
	}

	abd->abd_size = size;
	abd->abd_parent = sabd;
	refcount_create(&abd->abd_children);
	(void) refcount_add_many(&sabd->abd_children, abd->abd_size, abd);

	return (abd);
}

abd_t *
abd_get_offset(abd_t *sabd, size_t off)
{
	size_t size = sabd->abd_size > off ? sabd->abd_size - off : 0;

	VERIFY3U(size, >, 0);

	return (abd_get_offset_impl(sabd, off, size));
}

abd_t *
abd_get_offset_size(abd_t *sabd, size_t off, size_t size)
{
	ASSERT3U(off + size, <=, sabd->abd_size);

	return (abd_get_offset_impl(sabd, off, size));
}

/*
 * Allocate a linear ABD structure for buf. You must free this with abd_put()
 * since the resulting ABD doesn't own its own buffer.
 */
abd_t *
abd_get_from_buf(void *buf, size_t size)
{
	abd_t *abd = abd_alloc_struct();

	VERIFY3U(size, <=, SPA_MAXBLOCKSIZE);

	/*
	 * Even if this buf is filesystem metadata, we only track that if we
	 * own the underlying data buffer, which is not true in this case.
	 * Therefore, we don't ever use ABD_FLAG_META here.
	 */
	abd->abd_flags = ABD_FLAG_LINEAR;
	abd->abd_size = size;
	abd->abd_parent = NULL;
	refcount_create(&abd->abd_children);

	abd->abd_u.abd_linear.abd_buf = buf;

	return (abd);
}

/*
 * Free an ABD allocated from abd_get_offset() or abd_get_from_buf(). Will not
 * free the underlying scatterlist or buffer.
 */
void
abd_put(abd_t *abd)
{
	abd_verify(abd);
	ASSERT(!(abd->abd_flags & ABD_FLAG_OWNER));

	if (abd->abd_parent != NULL) {
		(void) refcount_remove_many(&abd->abd_parent->abd_children,
		    abd->abd_size, abd);
	}

	refcount_destroy(&abd->abd_children);
	abd_free_struct(abd);
}

/*
 * Get the raw buffer associated with a linear ABD.
 */
void *
abd_to_buf(abd_t *abd)
{
	ASSERT(abd_is_linear(abd));
	abd_verify(abd);
	return (abd->abd_u.abd_linear.abd_buf);
}

/*
 * Borrow a raw buffer from an ABD without copying the contents of the ABD
 * into the buffer. If the ABD is scattered, this will allocate a raw buffer
 * whose contents are undefined. To copy over the existing data in the ABD, use
 * abd_borrow_buf_copy() instead.
 */
void *
abd_borrow_buf(abd_t *abd, size_t n)
{
	void *buf;
	abd_verify(abd);
	ASSERT3U(abd->abd_size, >=, n);
	if (abd_is_linear(abd)) {
		buf = abd_to_buf(abd);
	} else {
		buf = zio_buf_alloc(n);
	}
	(void) refcount_add_many(&abd->abd_children, n, buf);

	return (buf);
}

void *
abd_borrow_buf_copy(abd_t *abd, size_t n)
{
	void *buf = abd_borrow_buf(abd, n);
	if (!abd_is_linear(abd)) {
		abd_copy_to_buf(buf, abd, n);
	}
	return (buf);
}

/*
 * Return a borrowed raw buffer to an ABD. If the ABD is scattered, this will
 * not change the contents of the ABD and will ASSERT that you didn't modify
 * the buffer since it was borrowed. If you want any changes you made to buf to
 * be copied back to abd, use abd_return_buf_copy() instead.
 */
void
abd_return_buf(abd_t *abd, void *buf, size_t n)
{
	abd_verify(abd);
	ASSERT3U(abd->abd_size, >=, n);
	if (abd_is_linear(abd)) {
		ASSERT3P(buf, ==, abd_to_buf(abd));
	} else {
		ASSERT0(abd_cmp_buf(abd, buf, n));
		zio_buf_free(buf, n);
	}
	(void) refcount_remove_many(&abd->abd_children, n, buf);
}

void
abd_return_buf_copy(abd_t *abd, void *buf, size_t n)
{
	if (!abd_is_linear(abd)) {
		abd_copy_from_buf(abd, buf, n);
	}
	abd_return_buf(abd, buf, n);
}

/*
 * Give this ABD ownership of the buffer that it's storing. Can only be used on
 * linear ABDs which were allocated via abd_get_from_buf(), or ones allocated
 * with abd_alloc_linear() which subsequently released ownership of their buf
 * with abd_release_ownership_of_buf().
 */
void
abd_take_ownership_of_buf(abd_t *abd, boolean_t is_metadata)
{
	ASSERT(abd_is_linear(abd));
	ASSERT(!(abd->abd_flags & ABD_FLAG_OWNER));
	abd_verify(abd);

	abd->abd_flags |= ABD_FLAG_OWNER;
	if (is_metadata) {
		abd->abd_flags |= ABD_FLAG_META;
	}

	ABDSTAT_BUMP(abdstat_linear_cnt);
	ABDSTAT_INCR(abdstat_linear_data_size, abd->abd_size);
}

void
abd_release_ownership_of_buf(abd_t *abd)
{
	ASSERT(abd_is_linear(abd));
	ASSERT(abd->abd_flags & ABD_FLAG_OWNER);
	abd_verify(abd);

	abd->abd_flags &= ~ABD_FLAG_OWNER;
	/* Disable this flag since we no longer own the data buffer */
	abd->abd_flags &= ~ABD_FLAG_META;

	ABDSTAT_BUMPDOWN(abdstat_linear_cnt);
	ABDSTAT_INCR(abdstat_linear_data_size, -(int)abd->abd_size);
}

#ifndef HAVE_1ARG_KMAP_ATOMIC
#define	NR_KM_TYPE (6)
#ifdef _KERNEL
int km_table[NR_KM_TYPE] = {
	KM_USER0,
	KM_USER1,
	KM_BIO_SRC_IRQ,
	KM_BIO_DST_IRQ,
	KM_PTE0,
	KM_PTE1,
};
#endif
#endif

struct abd_iter {
	/* public interface */
	void		*iter_mapaddr;	/* addr corresponding to iter_pos */
	size_t		iter_mapsize;	/* length of data valid at mapaddr */

	/* private */
	abd_t		*iter_abd;	/* ABD being iterated through */
	size_t		iter_pos;
	size_t		iter_offset;	/* offset in current sg/abd_buf, */
					/* abd_offset included */
	struct scatterlist *iter_sg;	/* current sg */
#ifndef HAVE_1ARG_KMAP_ATOMIC
	int		iter_km;	/* KM_* for kmap_atomic */
#endif
};

/*
 * Initialize the abd_iter.
 */
static void
abd_iter_init(struct abd_iter *aiter, abd_t *abd, int km_type)
{
	abd_verify(abd);
	aiter->iter_abd = abd;
	aiter->iter_mapaddr = NULL;
	aiter->iter_mapsize = 0;
	aiter->iter_pos = 0;
	if (abd_is_linear(abd)) {
		aiter->iter_offset = 0;
		aiter->iter_sg = NULL;
	} else {
		aiter->iter_offset = ABD_SCATTER(abd).abd_offset;
		aiter->iter_sg = ABD_SCATTER(abd).abd_sgl;
	}
#ifndef HAVE_1ARG_KMAP_ATOMIC
	ASSERT3U(km_type, <, NR_KM_TYPE);
	aiter->iter_km = km_type;
#endif
}

/*
 * Advance the iterator by a certain amount. Cannot be called when a chunk is
 * in use. This can be safely called when the aiter has already exhausted, in
 * which case this does nothing.
 */
static void
abd_iter_advance(struct abd_iter *aiter, size_t amount)
{
	ASSERT3P(aiter->iter_mapaddr, ==, NULL);
	ASSERT0(aiter->iter_mapsize);

	/* There's nothing left to advance to, so do nothing */
	if (aiter->iter_pos == aiter->iter_abd->abd_size)
		return;

	aiter->iter_pos += amount;
	aiter->iter_offset += amount;
	if (!abd_is_linear(aiter->iter_abd)) {
		while (aiter->iter_offset >= aiter->iter_sg->length) {
			aiter->iter_offset -= aiter->iter_sg->length;
			aiter->iter_sg = sg_next(aiter->iter_sg);
			if (aiter->iter_sg == NULL) {
				ASSERT0(aiter->iter_offset);
				break;
			}
		}
	}
}

/*
 * Map the current chunk into aiter. This can be safely called when the aiter
 * has already exhausted, in which case this does nothing.
 */
static void
abd_iter_map(struct abd_iter *aiter)
{
	void *paddr;
	size_t offset = 0;

	ASSERT3P(aiter->iter_mapaddr, ==, NULL);
	ASSERT0(aiter->iter_mapsize);

	/* There's nothing left to iterate over, so do nothing */
	if (aiter->iter_pos == aiter->iter_abd->abd_size)
		return;

	if (abd_is_linear(aiter->iter_abd)) {
		ASSERT3U(aiter->iter_pos, ==, aiter->iter_offset);
		offset = aiter->iter_offset;
		aiter->iter_mapsize = aiter->iter_abd->abd_size - offset;
		paddr = aiter->iter_abd->abd_u.abd_linear.abd_buf;
	} else {
		offset = aiter->iter_offset;
		aiter->iter_mapsize = MIN(aiter->iter_sg->length - offset,
		    aiter->iter_abd->abd_size - aiter->iter_pos);

		paddr = zfs_kmap_atomic(sg_page(aiter->iter_sg),
		    km_table[aiter->iter_km]);
	}

	aiter->iter_mapaddr = (char *)paddr + offset;
}

/*
 * Unmap the current chunk from aiter. This can be safely called when the aiter
 * has already exhausted, in which case this does nothing.
 */
static void
abd_iter_unmap(struct abd_iter *aiter)
{
	/* There's nothing left to unmap, so do nothing */
	if (aiter->iter_pos == aiter->iter_abd->abd_size)
		return;

	if (!abd_is_linear(aiter->iter_abd)) {
		/* LINTED E_FUNC_SET_NOT_USED */
		zfs_kunmap_atomic(aiter->iter_mapaddr - aiter->iter_offset,
		    km_table[aiter->iter_km]);
	}

	ASSERT3P(aiter->iter_mapaddr, !=, NULL);
	ASSERT3U(aiter->iter_mapsize, >, 0);

	aiter->iter_mapaddr = NULL;
	aiter->iter_mapsize = 0;
}

int
abd_iterate_func(abd_t *abd, size_t off, size_t size,
    abd_iter_func_t *func, void *private)
{
	int ret = 0;
	struct abd_iter aiter;

	abd_verify(abd);
	ASSERT3U(off + size, <=, abd->abd_size);

	abd_iter_init(&aiter, abd, 0);
	abd_iter_advance(&aiter, off);

	while (size > 0) {
		size_t len;
		abd_iter_map(&aiter);

		len = MIN(aiter.iter_mapsize, size);
		ASSERT3U(len, >, 0);

		ret = func(aiter.iter_mapaddr, len, private);

		abd_iter_unmap(&aiter);

		if (ret != 0)
			break;

		size -= len;
		abd_iter_advance(&aiter, len);
	}

	return (ret);
}

struct buf_arg {
	void *arg_buf;
};

static int
abd_copy_to_buf_off_cb(void *buf, size_t size, void *private)
{
	struct buf_arg *ba_ptr = private;

	(void) memcpy(ba_ptr->arg_buf, buf, size);
	ba_ptr->arg_buf = (char *)ba_ptr->arg_buf + size;

	return (0);
}

/*
 * Copy abd to buf. (off is the offset in abd.)
 */
void
abd_copy_to_buf_off(void *buf, abd_t *abd, size_t off, size_t size)
{
	struct buf_arg ba_ptr = { buf };

	(void) abd_iterate_func(abd, off, size, abd_copy_to_buf_off_cb,
	    &ba_ptr);
}

static int
abd_cmp_buf_off_cb(void *buf, size_t size, void *private)
{
	int ret;
	struct buf_arg *ba_ptr = private;

	ret = memcmp(buf, ba_ptr->arg_buf, size);
	ba_ptr->arg_buf = (char *)ba_ptr->arg_buf + size;

	return (ret);
}

/*
 * Compare the contents of abd to buf. (off is the offset in abd.)
 */
int
abd_cmp_buf_off(abd_t *abd, const void *buf, size_t off, size_t size)
{
	struct buf_arg ba_ptr = { (void *) buf };

	return (abd_iterate_func(abd, off, size, abd_cmp_buf_off_cb, &ba_ptr));
}

static int
abd_copy_from_buf_off_cb(void *buf, size_t size, void *private)
{
	struct buf_arg *ba_ptr = private;

	(void) memcpy(buf, ba_ptr->arg_buf, size);
	ba_ptr->arg_buf = (char *)ba_ptr->arg_buf + size;

	return (0);
}

/*
 * Copy from buf to abd. (off is the offset in abd.)
 */
void
abd_copy_from_buf_off(abd_t *abd, const void *buf, size_t off, size_t size)
{
	struct buf_arg ba_ptr = { (void *) buf };

	(void) abd_iterate_func(abd, off, size, abd_copy_from_buf_off_cb,
	    &ba_ptr);
}

/*ARGSUSED*/
static int
abd_zero_off_cb(void *buf, size_t size, void *private)
{
	(void) memset(buf, 0, size);
	return (0);
}

/*
 * Zero out the abd from a particular offset to the end.
 */
void
abd_zero_off(abd_t *abd, size_t off, size_t size)
{
	(void) abd_iterate_func(abd, off, size, abd_zero_off_cb, NULL);
}

/*
 * Iterate over two ABDs and call func incrementally on the two ABDs' data in
 * equal-sized chunks (passed to func as raw buffers). func could be called many
 * times during this iteration.
 */
int
abd_iterate_func2(abd_t *dabd, abd_t *sabd, size_t doff, size_t soff,
    size_t size, abd_iter_func2_t *func, void *private)
{
	int ret = 0;
	struct abd_iter daiter, saiter;

	abd_verify(dabd);
	abd_verify(sabd);

	ASSERT3U(doff + size, <=, dabd->abd_size);
	ASSERT3U(soff + size, <=, sabd->abd_size);

	abd_iter_init(&daiter, dabd, 0);
	abd_iter_init(&saiter, sabd, 1);
	abd_iter_advance(&daiter, doff);
	abd_iter_advance(&saiter, soff);

	while (size > 0) {
		size_t dlen, slen, len;
		abd_iter_map(&daiter);
		abd_iter_map(&saiter);

		dlen = MIN(daiter.iter_mapsize, size);
		slen = MIN(saiter.iter_mapsize, size);
		len = MIN(dlen, slen);
		ASSERT(dlen > 0 || slen > 0);

		ret = func(daiter.iter_mapaddr, saiter.iter_mapaddr, len,
		    private);

		abd_iter_unmap(&saiter);
		abd_iter_unmap(&daiter);

		if (ret != 0)
			break;

		size -= len;
		abd_iter_advance(&daiter, len);
		abd_iter_advance(&saiter, len);
	}

	return (ret);
}

/*ARGSUSED*/
static int
abd_copy_off_cb(void *dbuf, void *sbuf, size_t size, void *private)
{
	(void) memcpy(dbuf, sbuf, size);
	return (0);
}

/*
 * Copy from sabd to dabd starting from soff and doff.
 */
void
abd_copy_off(abd_t *dabd, abd_t *sabd, size_t doff, size_t soff, size_t size)
{
	(void) abd_iterate_func2(dabd, sabd, doff, soff, size,
	    abd_copy_off_cb, NULL);
}

/*ARGSUSED*/
static int
abd_cmp_cb(void *bufa, void *bufb, size_t size, void *private)
{
	return (memcmp(bufa, bufb, size));
}

/*
 * Compares the contents of two ABDs.
 */
int
abd_cmp(abd_t *dabd, abd_t *sabd)
{
	ASSERT3U(dabd->abd_size, ==, sabd->abd_size);
	return (abd_iterate_func2(dabd, sabd, 0, 0, dabd->abd_size,
	    abd_cmp_cb, NULL));
}

/*
 * Iterate over code ABDs and a data ABD and call @func_raidz_gen.
 *
 * @cabds          parity ABDs, must have equal size
 * @dabd           data ABD. Can be NULL (in this case @dsize = 0)
 * @func_raidz_gen should be implemented so that its behaviour
 *                 is the same when taking linear and when taking scatter
 */
void
abd_raidz_gen_iterate(abd_t **cabds, abd_t *dabd,
	ssize_t csize, ssize_t dsize, const unsigned parity,
	void (*func_raidz_gen)(void **, const void *, size_t, size_t))
{
	int i;
	ssize_t len, dlen;
	struct abd_iter caiters[3];
	struct abd_iter daiter;
	void *caddrs[3];
	unsigned long flags;

	ASSERT3U(parity, <=, 3);

	for (i = 0; i < parity; i++)
		abd_iter_init(&caiters[i], cabds[i], i);

	if (dabd)
		abd_iter_init(&daiter, dabd, i);

	ASSERT3S(dsize, >=, 0);

	local_irq_save(flags);
	while (csize > 0) {
		len = csize;

		if (dabd && dsize > 0)
			abd_iter_map(&daiter);

		for (i = 0; i < parity; i++) {
			abd_iter_map(&caiters[i]);
			caddrs[i] = caiters[i].iter_mapaddr;
		}

		switch (parity) {
			case 3:
				len = MIN(caiters[2].iter_mapsize, len);
			case 2:
				len = MIN(caiters[1].iter_mapsize, len);
			case 1:
				len = MIN(caiters[0].iter_mapsize, len);
		}

		/* must be progressive */
		ASSERT3S(len, >, 0);

		if (dabd && dsize > 0) {
			/* this needs precise iter.length */
			len = MIN(daiter.iter_mapsize, len);
			dlen = len;
		} else
			dlen = 0;

		/* must be progressive */
		ASSERT3S(len, >, 0);
		/*
		 * The iterated function likely will not do well if each
		 * segment except the last one is not multiple of 512 (raidz).
		 */
		ASSERT3U(((uint64_t)len & 511ULL), ==, 0);

		func_raidz_gen(caddrs, daiter.iter_mapaddr, len, dlen);

		for (i = parity-1; i >= 0; i--) {
			abd_iter_unmap(&caiters[i]);
			abd_iter_advance(&caiters[i], len);
		}

		if (dabd && dsize > 0) {
			abd_iter_unmap(&daiter);
			abd_iter_advance(&daiter, dlen);
			dsize -= dlen;
		}

		csize -= len;

		ASSERT3S(dsize, >=, 0);
		ASSERT3S(csize, >=, 0);
	}
	local_irq_restore(flags);
}

/*
 * Iterate over code ABDs and data reconstruction target ABDs and call
 * @func_raidz_rec. Function maps at most 6 pages atomically.
 *
 * @cabds           parity ABDs, must have equal size
 * @tabds           rec target ABDs, at most 3
 * @tsize           size of data target columns
 * @func_raidz_rec  expects syndrome data in target columns. Function
 *                  reconstructs data and overwrites target columns.
 */
void
abd_raidz_rec_iterate(abd_t **cabds, abd_t **tabds,
	ssize_t tsize, const unsigned parity,
	void (*func_raidz_rec)(void **t, const size_t tsize, void **c,
	const unsigned *mul),
	const unsigned *mul)
{
	int i;
	ssize_t len;
	struct abd_iter citers[3];
	struct abd_iter xiters[3];
	void *caddrs[3], *xaddrs[3];
	unsigned long flags;

	ASSERT3U(parity, <=, 3);

	for (i = 0; i < parity; i++) {
		abd_iter_init(&citers[i], cabds[i], 2*i);
		abd_iter_init(&xiters[i], tabds[i], 2*i+1);
	}

	local_irq_save(flags);
	while (tsize > 0) {

		for (i = 0; i < parity; i++) {
			abd_iter_map(&citers[i]);
			abd_iter_map(&xiters[i]);
			caddrs[i] = citers[i].iter_mapaddr;
			xaddrs[i] = xiters[i].iter_mapaddr;
		}

		len = tsize;
		switch (parity) {
			case 3:
				len = MIN(xiters[2].iter_mapsize, len);
				len = MIN(citers[2].iter_mapsize, len);
			case 2:
				len = MIN(xiters[1].iter_mapsize, len);
				len = MIN(citers[1].iter_mapsize, len);
			case 1:
				len = MIN(xiters[0].iter_mapsize, len);
				len = MIN(citers[0].iter_mapsize, len);
		}
		/* must be progressive */
		ASSERT3S(len, >, 0);
		/*
		 * The iterated function likely will not do well if each
		 * segment except the last one is not multiple of 512 (raidz).
		 */
		ASSERT3U(((uint64_t)len & 511ULL), ==, 0);

		func_raidz_rec(xaddrs, len, caddrs, mul);

		for (i = parity-1; i >= 0; i--) {
			abd_iter_unmap(&xiters[i]);
			abd_iter_unmap(&citers[i]);
			abd_iter_advance(&xiters[i], len);
			abd_iter_advance(&citers[i], len);
		}

		tsize -= len;
		ASSERT3S(tsize, >=, 0);
	}
	local_irq_restore(flags);
}

#if defined(_KERNEL) && defined(HAVE_SPL)
/*
 * bio_nr_pages for ABD.
 * @off is the offset in @abd
 */
unsigned long
abd_nr_pages_off(abd_t *abd, unsigned int size, size_t off)
{
	unsigned long pos;

	if (abd_is_linear(abd))
		pos = (unsigned long)abd_to_buf(abd) + off;
	else
		pos = abd->abd_u.abd_scatter.abd_offset + off;

	return ((pos + size + PAGESIZE - 1) >> PAGE_SHIFT)
					- (pos >> PAGE_SHIFT);
}

/*
 * bio_map for scatter ABD.
 * @off is the offset in @abd
 * Remaining IO size is returned
 */
unsigned int
abd_scatter_bio_map_off(struct bio *bio, abd_t *abd,
			unsigned int io_size, size_t off)
{
	int i;
	struct abd_iter aiter;

	ASSERT(!abd_is_linear(abd));
	ASSERT3U(io_size, <=, abd->abd_size - off);

	abd_iter_init(&aiter, abd, 0);
	abd_iter_advance(&aiter, off);

	for (i = 0; i < bio->bi_max_vecs; i++) {
		struct page *pg;
		size_t len, sgoff, pgoff;
		struct scatterlist *sg;

		if (io_size <= 0)
			break;

		sg = aiter.iter_sg;
		sgoff = aiter.iter_offset;
		pgoff = sgoff & (PAGESIZE - 1);
		len = MIN(io_size, PAGESIZE - pgoff);
		ASSERT(len > 0);

		pg = nth_page(sg_page(sg), sgoff >> PAGE_SHIFT);
		if (bio_add_page(bio, pg, len, pgoff) != len)
			break;

		io_size -= len;
		abd_iter_advance(&aiter, len);
	}

	return (io_size);
}

/* Tunable Parameters */
module_param(zfs_abd_scatter_enabled, int, 0644);
MODULE_PARM_DESC(zfs_abd_scatter_enabled,
	"Toggle whether ABD allocations must be linear.");
#endif
