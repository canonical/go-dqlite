#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include <sqlite3.h>

#include "format.h"
#include "vfs.h"

/* Maximum pathname length supported by this VFS. */
#define DQLITE__VFS_MAX_PATHNAME 512

/* Maximum number of files this VFS can create. */
#define DQLITE__VFS_MAX_FILES 64

/* Initialize a new volatile page for a database or WAL file.
 *
 * If it's a page for a WAL file, the WAL header will
 * also be allocated.
 */
static int dqlite__vfs_page_init(struct dqlite__vfs_page *p, int size, int wal) {
	assert(p != NULL);
	assert(size > 0);
	assert(wal == 0 || wal == 1);

	p->buf = sqlite3_malloc(size);
	if (p->buf == NULL) {
		goto err;
	}
	memset(p->buf, 0, size);

	if (wal) {
		p->hdr = sqlite3_malloc(DQLITE__FORMAT_WAL_FRAME_HDR_SIZE);
		if (p->hdr == NULL)
			goto err_after_buf_malloc;
		memset(p->hdr, 0, DQLITE__FORMAT_WAL_FRAME_HDR_SIZE);

		p->dirty_mask_size = size / sizeof(char);
		p->dirty_mask      = sqlite3_malloc(p->dirty_mask_size);
		if (p->dirty_mask == NULL) {
			goto err_after_hdr_malloc;
		}
		memset(p->dirty_mask, 0, p->dirty_mask_size);
	} else {
		p->hdr = NULL;
	}

	p->dirty_buf = NULL;

	return SQLITE_OK;

err_after_hdr_malloc:
	sqlite3_free(p->hdr);

err_after_buf_malloc:
	sqlite3_free(p->buf);

err:
	return SQLITE_NOMEM;
}

/* Release the memory of a volatile page */
static void dqlite__vfs_page_close(struct dqlite__vfs_page *p) {
	assert(p != NULL);
	assert(p->buf != NULL);

	sqlite3_free(p->buf);

	if (p->hdr != NULL) {
		assert(p->dirty_mask);

		sqlite3_free(p->hdr);
		sqlite3_free(p->dirty_mask);
	}
}

/* Initialize a new SHM mapping for a database file. */
static void dqlite__vfs_shm_init(struct dqlite__vfs_shm *s) {
	int i;

	s->regions     = NULL;
	s->regions_len = 0;
	s->refcount    = 0;

	for (i = 0; i < SQLITE_SHM_NLOCK; i++) {
		s->shared[i]    = 0;
		s->exclusive[i] = 0;
	}
}

/* Release the memory of a SHM mapping. */
static void dqlite__vfs_shm_close(struct dqlite__vfs_shm *s) {
	void *region;
	int   i;

	assert(s != NULL);

	/* Free all regions. */
	for (i = 0; i < s->regions_len; i++) {
		region = *(s->regions + i);
		assert(region != NULL);
		sqlite3_free(region);
	}

	/* Free the shared memory region array. */
	if (s->regions != NULL) {
		sqlite3_free(s->regions);
	}
}

/* Initialize the content structure for a new volatile file. */
static int dqlite__vfs_content_init(struct dqlite__vfs_content *c,
                                    const char *                filename,
                                    int                         type) {
	assert(c != NULL);
	assert(filename != NULL);
	assert(type == DQLITE__FORMAT_DB || type == DQLITE__FORMAT_WAL ||
	       type == DQLITE__FORMAT_OTHER);

	// Copy the name, since when called from Go, the pointer will be freed.
	c->filename = sqlite3_malloc(strlen(filename) + 1);
	if (c->filename == NULL) {
		goto err;
	}

	strcpy(c->filename, filename);

	// For WAL files, also allocate the WAL file header.
	if (type == DQLITE__FORMAT_WAL) {
		c->hdr = sqlite3_malloc(DQLITE__FORMAT_WAL_HDR_SIZE);
		if (c->hdr == NULL) {
			goto err_after_filename_malloc;
		}
		memset(c->hdr, 0, DQLITE__FORMAT_WAL_HDR_SIZE);
	} else {
		c->hdr = NULL;
	}

	c->pages     = 0;
	c->pages_len = 0;
	c->page_size = 0;
	c->refcount  = 0;
	c->type      = type;
	c->shm       = NULL;

	c->wal         = NULL;
	c->tx_refcount = 0;

	return SQLITE_OK;

err_after_filename_malloc:
	sqlite3_free(c->filename);

err:
	return SQLITE_NOMEM;
}

/* Release the memory used for the content of a volatile file. */
static void dqlite__vfs_content_close(struct dqlite__vfs_content *c, int force) {
	int                      i;
	struct dqlite__vfs_page *page;

	assert(c != NULL);
	assert(c->filename != NULL);
	assert(force == 0 || force == 1);

	/* If the force flag is not set, we expect callers to have checked that
	 * the refcount has dropped to zero. */
	assert(force == 1 || c->refcount == 0);

	/* Free the filename. */
	sqlite3_free(c->filename);

	/* Free the header if it's a WAL file. */
	if (c->type == DQLITE__FORMAT_WAL) {
		assert(c->hdr != NULL);
		sqlite3_free(c->hdr);
	} else {
		assert(c->hdr == NULL);
	}

	/* Free all pages. */
	for (i = 0; i < c->pages_len; i++) {
		page = *(c->pages + i);
		assert(page != NULL);
		dqlite__vfs_page_close(page);
		sqlite3_free(page);
	}

	/* Free the page array. */
	if (c->pages != NULL) {
		sqlite3_free(c->pages);
	}

	/* Free the SHM mappping */
	if (c->shm != NULL) {
		assert(c->type == DQLITE__FORMAT_DB);
		dqlite__vfs_shm_close(c->shm);
		sqlite3_free(c->shm);
	}
}

/* Return 1 if this file has no content. */
static int dqlite__vfs_content_is_empty(struct dqlite__vfs_content *c) {
	assert(c != NULL);

	if (c->pages_len == 0) {
		assert(c->pages == NULL);
		return 1;
	}

	// If it was written, a page list and a page size must have been set.
	assert(c->pages != NULL && c->pages_len > 0 && c->page_size > 0);

	return 0;
}

// Get a page from this file, possibly creating a new one.
static int dqlite__vfs_content_page_get(struct dqlite__vfs_content *c,
                                        int                         pgno,
                                        struct dqlite__vfs_page **  page) {
	int rc;
	int is_wal;

	assert(c != NULL);
	assert(pgno > 0);

	is_wal = c->type == DQLITE__FORMAT_WAL;

	/* SQLite should access pages progressively, without jumping more than one
	 * page after the end. */
	if (pgno > (c->pages_len + 1)) {
		rc = SQLITE_PROTOCOL;
		goto err;
	}

	if (pgno == (c->pages_len + 1)) {
		/* Create a new page, grow the page array, and append the
		 * new page to it. */
		struct dqlite__vfs_page **pages; /* New page array. */

		/* We assume that the page size has been set, either by
		 * intercepting the first main database file write, or by
		 * handling a 'PRAGMA page_size=N' command in
		 * dqlite__vfs_file_control(). */
		if (c->page_size == 0) {
			rc = SQLITE_PROTOCOL;
			goto err;
		}

		*page = sqlite3_malloc(sizeof **page);
		if (page == NULL) {
			rc = SQLITE_OK;
			goto err;
		}

		rc = dqlite__vfs_page_init(*page, c->page_size, is_wal);
		if (rc != SQLITE_OK) {
			assert(rc == SQLITE_NOMEM);
			goto err_after_page_malloc;
		}

		pages = sqlite3_realloc(c->pages, (sizeof *pages) * pgno);
		if (pages == NULL) {
			rc = SQLITE_NOMEM;
			goto err_after_page_init;
		}

		/* Append the new page to the new page array. */
		*(pages + pgno - 1) = *page;

		/* Update the page array. */
		c->pages     = pages;
		c->pages_len = pgno;
	} else {
		/* Return the existing page. */
		assert(c->pages != NULL);
		*page = *(c->pages + pgno - 1);
	}

	return SQLITE_OK;

err_after_page_init:
	dqlite__vfs_page_close(*page);

err_after_page_malloc:
	sqlite3_free(page);

err:
	assert(rc != SQLITE_OK);

	*page = NULL;

	return rc;
}

/* Lookup a page from this file, returning NULL if it doesn't exist. */
static struct dqlite__vfs_page *
dqlite__vfs_content_page_lookup(struct dqlite__vfs_content *c, int pgno) {
	struct dqlite__vfs_page *page;

	assert(c != NULL);
	assert(pgno > 0);

	if (pgno > c->pages_len) {
		/* This page hasn't been written yet. */
		return NULL;
	}

	page = *(c->pages + pgno - 1);

	assert(page != NULL);

	if (c->type == DQLITE__FORMAT_WAL)
		assert(page->hdr != NULL);

	return page;
}

/* Truncate the file to be exactly the given number of pages. */
static void dqlite__vfs_content_truncate(struct dqlite__vfs_content *content,
                                         int                         pages_len) {
	struct dqlite__vfs_page **cursor;
	int                       i;

	/* We expect callers to only invoke us if some actual content has been
	 * written already. */
	assert(content->pages_len > 0);

	/* Truncate should always shrink a file. */
	assert(pages_len <= content->pages_len);
	assert(content->pages != NULL);

	/* Destroy pages beyond pages_len. */
	cursor = content->pages + pages_len;
	for (i = 0; i < (content->pages_len - pages_len); i++) {
		dqlite__vfs_page_close(*cursor);
		sqlite3_free(*cursor);
		cursor++;
	}

	/* Reset the file header (for WAL files). */
	if (content->type == DQLITE__FORMAT_WAL) {
		assert(content->hdr != NULL);
		memset(content->hdr, 0, DQLITE__FORMAT_WAL_HDR_SIZE);
	} else {
		assert(content->hdr == NULL);
	}

	/* Shrink the page array, possibly to 0.
	 *
	 * TODO: in principle realloc could fail also when shrinking. */
	content->pages =
	    sqlite3_realloc(content->pages, (sizeof *(content->pages)) * pages_len);

	/* Update the page count. */
	content->pages_len = pages_len;
}

/* Initialize a new dqlite__vfs_root object. */
static int dqlite__vfs_root_init(struct dqlite__vfs_root *r) {
	int err;
	int contents_size =
	    sizeof(struct dqlite__vfs_content *) * DQLITE__VFS_MAX_FILES;

	assert(r != NULL);

	r->contents_len = DQLITE__VFS_MAX_FILES;

	r->contents = sqlite3_malloc(contents_size);
	if (r->contents == NULL) {
		return SQLITE_NOMEM;
	}
	memset(r->contents, 0, contents_size);

	err = pthread_mutex_init(&r->mutex, NULL);
	assert(err == 0); /* Docs say that pthread_mutex_init can't fail */

	return SQLITE_OK;
}

/* Release the memory used internally by dqlite__vfs_root object.
 *
 * All file content will be de-allocated, so dangling open FDs against
 * those files will be broken.
 */
static void dqlite__vfs_root_close(struct dqlite__vfs_root *r) {
	struct dqlite__vfs_content **cursor; /* Iterator for r->contents */
	int                          i;

	assert(r != NULL);
	assert(r->contents != NULL);

	cursor = r->contents;

	/* The content array has been allocated and has at least one slot. */
	assert(cursor != NULL);
	assert(r->contents_len > 0);

	for (i = 0; i < r->contents_len; i++) {
		struct dqlite__vfs_content *content = *cursor;
		if (content != NULL) {
			dqlite__vfs_content_close(content, 1);
			sqlite3_free(content);
		}
		cursor++;
	}

	sqlite3_free(r->contents);
}

/* Find a content object by name.
 *
 * Fill out and return its index if found, otherwise return the index
 * of a free slot (or -1, if there are no free slots).
 */
static int dqlite__vfs_root_content_lookup(
    struct dqlite__vfs_root *    r,
    const char *                 filename,
    struct dqlite__vfs_content **out // OUT: content object or NULL
) {
	struct dqlite__vfs_content **cursor; /* Iterator for r->contents */
	int                          i;

	/* Index of the content or of a free slot in the contents array. */
	int free_slot = -1;

	assert(r != NULL);
	assert(filename != NULL);

	cursor = r->contents;

	// The content array has been allocated and has at least one slot.
	assert(cursor != NULL);
	assert(r->contents_len > 0);

	for (i = 0; i < r->contents_len; i++) {
		struct dqlite__vfs_content *content = *cursor;
		if (content && strcmp(content->filename, filename) == 0) {
			// Found matching file.
			*out = content;
			return i;
		}
		if (content == NULL && free_slot == -1) {
			// Keep track of the index of this empty slot.
			free_slot = i;
		}
		cursor++;
	}

	// No matching content object.
	*out = 0;

	return free_slot;
}

/* Find the database content object associated with the given WAL file name. */
static int
dqlite__vfs_root_database_content_lookup(struct dqlite__vfs_root *    r,
                                         const char *                 wal_filename,
                                         struct dqlite__vfs_content **out) {
	struct dqlite__vfs_content *content;
	int                         main_filename_len;
	char *                      main_filename;

	assert(r != NULL);
	assert(wal_filename != NULL);
	assert(out != NULL);

	*out = NULL; /* In case of errors */

	main_filename_len = strlen(wal_filename) - strlen("-wal") + 1;
	main_filename     = sqlite3_malloc(main_filename_len);

	if (main_filename == NULL) {
		return SQLITE_NOMEM;
	}

	strncpy(main_filename, wal_filename, main_filename_len - 1);
	main_filename[main_filename_len - 1] = '\0';

	dqlite__vfs_root_content_lookup(r, main_filename, &content);

	sqlite3_free(main_filename);

	if (content == NULL) {
		return SQLITE_CORRUPT;
	}

	*out = content;

	return SQLITE_OK;
}

/* Return the size of the database file whose WAL file has the given name.
 *
 * The size must have been previously set when this routine is called. */
static int dqlite__vfs_root_database_page_size(struct dqlite__vfs_root *r,
                                               const char *             wal_filename,
                                               unsigned int *           page_size) {
	struct dqlite__vfs_content *content;
	int                         err;

	assert(r != NULL);
	assert(wal_filename != NULL);
	assert(page_size != NULL);

	*page_size = 0; /* In case of errors. */

	err = dqlite__vfs_root_database_content_lookup(r, wal_filename, &content);
	if (err != SQLITE_OK) {
		return err;
	}

	assert(content->page_size > 0);

	*page_size = content->page_size;

	return SQLITE_OK;
}

static int dqlite__vfs_close(sqlite3_file *file) {
	struct dqlite__vfs_file *f    = (struct dqlite__vfs_file *)file;
	struct dqlite__vfs_root *root = (struct dqlite__vfs_root *)(f->root);

	pthread_mutex_lock(&root->mutex);

	assert(f->content->refcount);
	f->content->refcount--;

	pthread_mutex_unlock(&root->mutex);

	return SQLITE_OK;
}

static int
dqlite__vfs_read(sqlite3_file *file, void *buf, int amount, sqlite_int64 offset) {
	struct dqlite__vfs_file *f = (struct dqlite__vfs_file *)file;

	int                      pgno;
	struct dqlite__vfs_page *page;

	assert(buf != NULL);
	assert(amount > 0);
	assert(f != NULL);
	assert(f->content != NULL);
	assert(f->content->filename != NULL);
	assert(f->content->refcount > 0);

	/* From SQLite docs:
	 *
	 *   If xRead() returns SQLITE_IOERR_SHORT_READ it must also fill
	 *   in the unread portions of the buffer with zeros.  A VFS that
	 *   fails to zero-fill short reads might seem to work.  However,
	 *   failure to zero-fill short reads will eventually lead to
	 *   database corruption.
	 */

	/* Check if the file is empty. */
	if (dqlite__vfs_content_is_empty(f->content)) {
		memset(buf, 0, amount);
		return SQLITE_IOERR_SHORT_READ;
	}

	/* From this point on we can assume that the file was written at least
	 * once. */

	/* Since writes to all files other than the main database or the WAL are
	 * no-ops and the associated content object remains empty, we expect the
	 * content type to be either DQLITE__FORMAT_DB or
	 * DQLITE__FORMAT_WAL. */
	assert(f->content->type == DQLITE__FORMAT_DB ||
	       f->content->type == DQLITE__FORMAT_WAL);

	switch (f->content->type) {

	case DQLITE__FORMAT_DB:
		/* Main database */

		/* If the main database file is not empty, we expect the page size
		 * to have been set by an initial write. */
		assert(f->content->page_size > 0);

		if (offset < f->content->page_size) {
			/* Reading from page 1. We expect the read to be at most
			 * page_size bytes. */
			assert(amount <= (int)f->content->page_size);

			pgno = 1;
		} else {
			/* For pages greater than 1, we expect a full page read, with
			 * an offset that starts exectly at the page boundary. */
			assert(amount == (int)f->content->page_size);
			assert((offset % f->content->page_size) == 0);

			pgno = (offset / f->content->page_size) + 1;
		}

		assert(pgno > 0);

		page = dqlite__vfs_content_page_lookup(f->content, pgno);

		if (pgno == 1) {
			/* Read the desired part of page 1. */
			memcpy(buf, page->buf + offset, amount);
		} else {
			/* Read the full page. */
			memcpy(buf, page->buf, amount);
		}
		return SQLITE_OK;

	case DQLITE__FORMAT_WAL:
		/* WAL file */

		if (f->content->page_size == 0) {
			/* If the page size hasn't been set yet, set it by copy the
			 * one from the associated main database file. */
			int err = dqlite__vfs_root_database_page_size(
			    f->root, f->content->filename, &f->content->page_size);
			if (err != 0) {
				return err;
			}
		}

		if (offset == 0) {
			/* Read the header. */
			assert(amount == DQLITE__FORMAT_WAL_HDR_SIZE);
			assert(f->content->hdr);
			memcpy(buf, f->content->hdr, DQLITE__FORMAT_WAL_HDR_SIZE);
			return SQLITE_OK;
		}

		/* For any other frame, we expect either a header read, a
		 * checksum read, a page read or a full frame read. */
		if (amount == DQLITE__FORMAT_WAL_FRAME_HDR_SIZE) {
			assert(((offset - DQLITE__FORMAT_WAL_HDR_SIZE) %
			        (f->content->page_size +
			         DQLITE__FORMAT_WAL_FRAME_HDR_SIZE)) == 0);
			pgno = dqlite__format_wal_calc_pgno(f->content->page_size,
			                                    offset);
		} else if (amount == sizeof(uint32_t) * 2) {
			if (offset == DQLITE__FORMAT_WAL_FRAME_HDR_SIZE) {
				/* Read the checksum from the WAL header. */
				memcpy(buf, f->content->hdr + offset, amount);
				return SQLITE_OK;
			}
			assert(((offset - 16 - DQLITE__FORMAT_WAL_HDR_SIZE) %
			        (f->content->page_size +
			         DQLITE__FORMAT_WAL_FRAME_HDR_SIZE)) == 0);
			pgno = (offset - 16 - DQLITE__FORMAT_WAL_HDR_SIZE) /
			           (f->content->page_size +
			            DQLITE__FORMAT_WAL_FRAME_HDR_SIZE) +
			       1;
		} else if (amount == (int)f->content->page_size) {
			assert(((offset - DQLITE__FORMAT_WAL_HDR_SIZE -
			         DQLITE__FORMAT_WAL_FRAME_HDR_SIZE) %
			        (f->content->page_size +
			         DQLITE__FORMAT_WAL_FRAME_HDR_SIZE)) == 0);
			pgno = dqlite__format_wal_calc_pgno(f->content->page_size,
			                                    offset);
		} else {
			assert(amount == (DQLITE__FORMAT_WAL_FRAME_HDR_SIZE +
			                  (int)f->content->page_size));
			pgno = dqlite__format_wal_calc_pgno(f->content->page_size,
			                                    offset);
		}

		if (pgno == 0) {
			// This is an attempt to read a page that was never written.
			memset(buf, 0, amount);
			return SQLITE_IOERR_SHORT_READ;
		}

		page = dqlite__vfs_content_page_lookup(f->content, pgno);

		if (amount == DQLITE__FORMAT_WAL_FRAME_HDR_SIZE) {
			memcpy(buf, page->hdr, amount);
		} else if (amount == sizeof(uint32_t) * 2) {
			memcpy(buf, page->hdr + 16, amount);
		} else if (amount == (int)f->content->page_size) {
			memcpy(buf, page->buf, amount);
		} else {
			memcpy(buf, page->hdr, DQLITE__FORMAT_WAL_FRAME_HDR_SIZE);
			memcpy(buf + DQLITE__FORMAT_WAL_FRAME_HDR_SIZE,
			       page->buf,
			       f->content->page_size);
		}

		return SQLITE_OK;
	}

	return SQLITE_IOERR_READ;
}

static int dqlite__vfs_write(sqlite3_file *file,
                             const void *  buf,
                             int           amount,
                             sqlite_int64  offset) {
	struct dqlite__vfs_file *f = (struct dqlite__vfs_file *)file;

	unsigned                 pgno;
	struct dqlite__vfs_page *page;
	int                      rc;

	assert(buf != NULL);
	assert(amount > 0);
	assert(f != NULL);
	assert(f->content != NULL);
	assert(f->content->filename != NULL);
	assert(f->content->refcount > 0);

	switch (f->content->type) {
	case DQLITE__FORMAT_DB:
		/* Main database. */
		if (offset == 0) {
			unsigned int page_size;

			/* This is the first database page. We expect the data to
			 * contain at least the header. */
			assert(amount >= DQLITE__FORMAT_DB_HDR_SIZE);

			/* Extract the page size from the header. */
			rc = dqlite__format_get_page_size(
			    DQLITE__FORMAT_DB, buf, &page_size);
			if (rc != SQLITE_OK) {
				return rc;
			}

			if (f->content->page_size > 0) {
				/* Check that the given page size actually matches
				 * what we have recorded. Since we make 'PRAGMA
				 * page_size=N' fail if the page is already set (see
				 * struct dqlite__vfs_fileControl), there should be
				 * no way for the user to change it. */
				assert(page_size == f->content->page_size);
			} else {
				/* This must be the very first write to the database.
				 * Keep track of the page size. */
				f->content->page_size = page_size;
			}

			pgno = 1;
		} else {
			/* The header must have been written and the page size set.
			 */
			assert(f->content->page_size > 0);

			/* For pages beyond the first we expect offset to be a
			 * multiple of the page size. */
			assert((offset % f->content->page_size) == 0);

			/* We expect that SQLite writes a page at time. */
			assert(amount == (int)f->content->page_size);

			pgno = (offset / f->content->page_size) + 1;
		}

		dqlite__vfs_content_page_get(f->content, pgno, &page);
		if (page == NULL) {
			return SQLITE_NOMEM;
		}

		assert(page->buf != NULL);

		memcpy(page->buf, buf, amount);

		return SQLITE_OK;

	case DQLITE__FORMAT_WAL:
		/* WAL file. */

		if (f->content->page_size == 0) {
			/* If the page size hasn't been set yet, set it by copy the
			 * one from the associated main database file. */
			int err = dqlite__vfs_root_database_page_size(
			    f->root, f->content->filename, &f->content->page_size);
			if (err != 0) {
				return err;
			}
		}

		if (offset == 0) {
			/* This is the WAL header. */
			unsigned int page_size;
			int          rc;

			/* We expect the data to contain exactly 32 bytes. */
			assert(amount == DQLITE__FORMAT_WAL_HDR_SIZE);

			/* The page size indicated in the header must be valid
			 * and match the one of the database file. */
			rc = dqlite__format_get_page_size(
			    DQLITE__FORMAT_WAL, buf, &page_size);
			if (rc != SQLITE_OK) {
				return SQLITE_CORRUPT;
			}

			if (page_size != f->content->page_size) {
				return SQLITE_CORRUPT;
			}

			memcpy(f->content->hdr, buf, amount);
			return SQLITE_OK;
		}

		assert(f->content->page_size > 0);

		/* This is a WAL frame write. We expect either a frame header or page
		 * write. */
		if (amount == DQLITE__FORMAT_WAL_FRAME_HDR_SIZE) {
			/* Frame header write. */
			assert(((offset - DQLITE__FORMAT_WAL_HDR_SIZE) %
			        (f->content->page_size +
			         DQLITE__FORMAT_WAL_FRAME_HDR_SIZE)) == 0);

			pgno = dqlite__format_wal_calc_pgno(f->content->page_size,
			                                    offset);

			dqlite__vfs_content_page_get(f->content, pgno, &page);
			if (page == NULL) {
				return SQLITE_NOMEM;
			}
			memcpy(page->hdr, buf, amount);
		} else {
			/* Frame page write. */
			assert(amount == (int)f->content->page_size);
			assert(((offset - DQLITE__FORMAT_WAL_HDR_SIZE -
			         DQLITE__FORMAT_WAL_FRAME_HDR_SIZE) %
			        (f->content->page_size +
			         DQLITE__FORMAT_WAL_FRAME_HDR_SIZE)) == 0);

			pgno = dqlite__format_wal_calc_pgno(f->content->page_size,
			                                    offset);

			// The header for the this frame must already have been
			// written, so the page is there.
			page = dqlite__vfs_content_page_lookup(f->content, pgno);

			assert(page != NULL);

			memcpy(page->buf, buf, amount);
		}

		return SQLITE_OK;

	case DQLITE__FORMAT_OTHER:
		// Silently swallow writes to any other file.
		return SQLITE_OK;
	}

	return SQLITE_IOERR_WRITE;
}

static int dqlite__vfs_truncate(sqlite3_file *file, sqlite_int64 size) {
	struct dqlite__vfs_file *f = (struct dqlite__vfs_file *)file;
	int                      pgno;

	assert(f != NULL);
	assert(f->content != NULL);

	/* We expect calls to xTruncate only for database and WAL files. */
	if (f->content->type != DQLITE__FORMAT_DB &&
	    f->content->type != DQLITE__FORMAT_WAL) {
		return SQLITE_PROTOCOL;
	}

	/* Check if this file empty.*/
	if (dqlite__vfs_content_is_empty(f->content)) {
		if (size > 0) {
			/* We don't expect SQLite to grow empty files. */
			return SQLITE_PROTOCOL;
		}

		/* Nothing to do. */
		return SQLITE_OK;
	}

	switch (f->content->type) {
	case DQLITE__FORMAT_DB:
		/* Main database. */
		if (f->content->page_size == 0) {
			return SQLITE_PROTOCOL;
		}
		if ((size % f->content->page_size) != 0) {
			return SQLITE_PROTOCOL;
		}
		pgno = size / f->content->page_size;
		break;

	case DQLITE__FORMAT_WAL:
		/* WAL file. */

		/* We expect SQLite to only truncate to zero, after a full
		 * checkpoint.
		 *
		 * TODO: figure out other case where SQLite might truncate to a
		 *       different size.
		 */
		if (size != 0) {
			return SQLITE_PROTOCOL;
		}
		pgno = 0;
		break;
	}

	dqlite__vfs_content_truncate(f->content, pgno);

	return SQLITE_OK;
}

static int dqlite__vfs_sync(sqlite3_file *file, int flags) {
	(void)file;
	(void)flags;

	return SQLITE_OK;
}

static int dqlite__vfs_file_size(sqlite3_file *file, sqlite_int64 *size) {
	struct dqlite__vfs_file *f = (struct dqlite__vfs_file *)file;

	/* Check if this file empty. */
	if (dqlite__vfs_content_is_empty(f->content)) {
		*size = 0;
		return SQLITE_OK;
	}

	/* Since we don't allow writing any other file, this must be
	 * either a database file or WAL file. */
	assert(f->content->type == DQLITE__FORMAT_DB ||
	       f->content->type == DQLITE__FORMAT_WAL);

	/* Since this file is not empty, the page size must have been set. */
	assert(f->content->page_size > 0);

	switch (f->content->type) {
	case DQLITE__FORMAT_DB:
		*size = f->content->pages_len * f->content->page_size;
		break;

	case DQLITE__FORMAT_WAL:
		/* TODO? here we assume that FileSize() is never invoked between
		 *       a header write and a page write. */
		*size = DQLITE__FORMAT_WAL_HDR_SIZE +
		        (f->content->pages_len * (DQLITE__FORMAT_WAL_FRAME_HDR_SIZE +
		                                  f->content->page_size));
		break;
	}

	return SQLITE_OK;
}

/* Locking a file is a no-op, since no other process has visibility on it. */
static int dqlite__vfs_lock(sqlite3_file *file, int lock) {
	(void)file;
	(void)lock;

	return SQLITE_OK;
}

/* Unlocking a file is a no-op, since no other process has visibility on it. */
static int dqlite__vfs_unlock(sqlite3_file *file, int lock) {
	(void)file;
	(void)lock;

	return SQLITE_OK;
}

/* We always report that a lock is held. This routine should be used only in
 * journal mode, so it doesn't matter. */
static int dqlite__vfs_check_reserved_lock(sqlite3_file *file, int *result) {
	(void)file;

	*result = 1;
	return SQLITE_OK;
}

/* Handle pragma a pragma file control. See the xFileControl
 * docstring in sqlite.h.in for more details. */
static int dqlite__vfs_file_control_pragma(struct dqlite__vfs_file *f,
                                           char **                  fnctl) {
	const char *left;
	const char *right;

	assert(f != NULL);
	assert(fnctl != NULL);

	left  = fnctl[1];
	right = fnctl[2];

	assert(left != NULL);

	if (strcmp(left, "page_size") == 0 && right) {
		/* When the user executes 'PRAGMA page_size=N' we save the
		 * size internally.
		 *
		 * The page size must be between 512 and 65536, and be a
		 * power of two. The check below was copied from
		 * sqlite3BtreeSetPageSize in btree.c.
		 *
		 * Invalid sizes are simply ignored, SQLite will do the same.
		 *
		 * It's not possible to change the size after it's set.
		 */
		int page_size = atoi(right);

		if (page_size >= DQLITE__FORMAT_PAGE_SIZE_MIN &&
		    page_size <= DQLITE__FORMAT_PAGE_SIZE_MAX &&
		    ((page_size - 1) & page_size) == 0) {

			if (f->content->page_size &&
			    page_size != (int)f->content->page_size) {
				fnctl[0] = "changing page size is not supported";
				return SQLITE_ERROR;
			}
			f->content->page_size = page_size;
		}
	} else if (strcmp(left, "journal_mode") == 0 && right) {
		/* When the user executes 'PRAGMA journal_mode=x' we ensure
		 * that the desired mode is 'wal'. */
		if (strcasecmp(right, "wal") != 0) {
			fnctl[0] = "only WAL mode is supported";
			return SQLITE_ERROR;
		}
	}

	return SQLITE_NOTFOUND;
}

static int dqlite__vfs_file_control_wal_idx_mx_frame(struct dqlite__vfs_file *f,
                                                     uint32_t *               v) {
	uint32_t *idx;

	assert(f != NULL);
	assert(v != NULL);

	assert(f->content->type == DQLITE__FORMAT_DB);
	assert(f->content->shm != NULL);

	assert(f->content->shm->regions != NULL);
	assert(f->content->shm->regions_len > 0);

	idx = f->content->shm->regions[0];

	/* The mxFrame number is 16th byte of the WAL index header. See also
	 * https://sqlite.org/walformat.html. */
	*v = idx[4];

	return SQLITE_OK;
}

static int dqlite__vfs_file_control_wal_idx_read_marks(struct dqlite__vfs_file *f,
                                                       uint32_t v[5]) {
	uint32_t *idx;

	assert(f != NULL);
	assert(v != NULL);

	assert(f->content->type == DQLITE__FORMAT_DB);
	assert(f->content->shm != NULL);

	assert(f->content->shm->regions != NULL);
	assert(f->content->shm->regions_len > 0);

	idx = f->content->shm->regions[0];

	/* The read-mark array starts at the 100th byte of the WAL index
	 * header. See also https://sqlite.org/walformat.html. */
	memcpy(v, &idx[25], (sizeof *idx) * 5);

	return SQLITE_OK;
}

static int dqlite__vfs_file_control(sqlite3_file *file, int op, void *arg) {
	struct dqlite__vfs_file *f = (struct dqlite__vfs_file *)file;

	switch (op) {

	case SQLITE_FCNTL_PRAGMA:
		return dqlite__vfs_file_control_pragma(f, arg);

	case DQLITE__VFS_FCNTL_WAL_IDX_MX_FRAME:
		return dqlite__vfs_file_control_wal_idx_mx_frame(f, arg);

	case DQLITE__VFS_FCNTL_WAL_IDX_READ_MARKS:
		return dqlite__vfs_file_control_wal_idx_read_marks(f, arg);
	}

	return SQLITE_OK;
}

static int dqlite__vfs_sector_size(sqlite3_file *file) {
	(void)file;

	return 0;
}

static int dqlite__vfs_device_characteristics(sqlite3_file *file) {
	(void)file;

	return 0;
}

/* Simulate shared memory by allocating on the C heap. */
static int dqlite__vfs_shm_map(sqlite3_file *file, /* Handle open on database file */
                               int           region_index, /* Region to retrieve */
                               int           region_size,  /* Size of regions */
                               int extend, /* True to extend file if necessary */
                               void volatile **out /* OUT: Mapped memory */
) {
	struct dqlite__vfs_file *f = (struct dqlite__vfs_file *)file;
	void *                   region;
	int                      rc;

	if (f->content->shm == NULL) {
		f->content->shm = sqlite3_malloc(sizeof *f->content->shm);
		if (f->content->shm == NULL) {
			rc = SQLITE_NOMEM;
			goto err;
		}
		dqlite__vfs_shm_init(f->content->shm);
	}

	if (f->content->shm->regions != NULL &&
	    region_index < f->content->shm->regions_len) {
		/* The region was already allocated. */
		region = *(f->content->shm->regions + region_index);
		assert(region != NULL);
	} else {
		if (extend) {
			/* We should grow the map one region at a time. */
			assert(region_index == f->content->shm->regions_len);
			region = sqlite3_malloc(region_size);
			if (region == NULL) {
				rc = SQLITE_NOMEM;
				goto err;
			}

			memset(region, 0, region_size);

			f->content->shm->regions =
			    sqlite3_realloc(f->content->shm->regions,
			                    sizeof(void *) * (region_index + 1));

			if (f->content->shm->regions == NULL) {
				rc = SQLITE_NOMEM;
				goto err_after_region_malloc;
			}

			*(f->content->shm->regions + region_index) = region;
			f->content->shm->regions_len++;

		} else {
			/* The region was not allocated and we don't have to
			 * extend the map. */
			region = NULL;
		}
	}

	if (region) {
		f->content->shm->refcount++;
	}

	*out = region;

	return SQLITE_OK;

err_after_region_malloc:
	sqlite3_free(region);

err:
	assert(rc != SQLITE_OK);

	*out = NULL;

	return rc;
}

static int dqlite__vfs_shm_lock(sqlite3_file *file, int ofst, int n, int flags) {
	struct dqlite__vfs_file *f;
	int                      i;

	assert(file != NULL);

	/* Legal values for the offset and the range */
	assert(ofst >= 0 && ofst + n <= SQLITE_SHM_NLOCK);
	assert(n >= 1);
	assert(n == 1 || (flags & SQLITE_SHM_EXCLUSIVE) != 0);

	/* Legal values for the flags.
	 *
	 * See https://sqlite.org/c3ref/c_shm_exclusive.html. */
	assert(flags == (SQLITE_SHM_LOCK | SQLITE_SHM_SHARED) ||
	       flags == (SQLITE_SHM_LOCK | SQLITE_SHM_EXCLUSIVE) ||
	       flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_SHARED) ||
	       flags == (SQLITE_SHM_UNLOCK | SQLITE_SHM_EXCLUSIVE));

	/* This is a no-op since shared-memory locking is relevant only for
	 * inter-process concurrency. See also the unix-excl branch from upstream
	 * (git commit cda6b3249167a54a0cf892f949d52760ee557129). */

	f = (struct dqlite__vfs_file *)file;

	assert(f->content != NULL);
	assert(f->content->shm != NULL);

	if (flags & SQLITE_SHM_UNLOCK) {
		unsigned *these_locks;
		unsigned *other_locks;

		if (flags & SQLITE_SHM_SHARED) {
			these_locks = f->content->shm->shared;
			other_locks = f->content->shm->exclusive;
		} else {
			these_locks = f->content->shm->exclusive;
			other_locks = f->content->shm->shared;
		}

		for (i = ofst; i < ofst + n; i++) {
			/* Sanity check that no lock of the other type is held
			 * in this region. */
			assert(other_locks[i] == 0);

			/* Sanity check that there is at least one lock held for
			 * this index. */
			assert(these_locks[i] > 0);

			these_locks[i]--;
		}
	} else {
		if (flags & SQLITE_SHM_EXCLUSIVE) {
			/* No shared or exclusive lock must be held in the region. */
			for (i = ofst; i < ofst + n; i++) {
				if (f->content->shm->shared[i] > 0 ||
				    f->content->shm->exclusive[i] > 0) {
					return SQLITE_BUSY;
				}
			}

			for (i = ofst; i < ofst + n; i++) {
				assert(f->content->shm->exclusive[i] == 0);
				f->content->shm->exclusive[i] = 1;
			}
		} else {
			/* No exclusive lock must be held in the region. */
			for (i = ofst; i < ofst + n; i++) {
				if (f->content->shm->exclusive[i] > 0) {
					return SQLITE_BUSY;
				}
			}

			for (i = ofst; i < ofst + n; i++) {
				f->content->shm->shared[i]++;
			}
		}
	}

	return SQLITE_OK;
}

static void dqlite__vfs_shm_barrier(sqlite3_file *file) {
	(void)file;
	/* This is a no-op since we expect SQLite to be compiled with mutex
	 * support (i.e. SQLITE_MUTEX_OMIT or SQLITE_MUTEX_NOOP are *not*
	 * defined, see sqliteInt.h). */
}

static int dqlite__vfs_shm_unmap(sqlite3_file *file, int delete_flag) {
	struct dqlite__vfs_file *f;

	assert(file != NULL);

	(void)delete_flag;

	f = (struct dqlite__vfs_file *)file;

	// If the shm pointer is NULL shared memory mapping is set.
	if (f->content->shm == NULL) {
		return SQLITE_OK;
	}

	f->content->shm->refcount--;

	/* If we got zero references, free the entire map. */
	if (f->content->shm->refcount == 0) {
		dqlite__vfs_shm_close(f->content->shm);
		sqlite3_free(f->content->shm);
		f->content->shm = NULL;
	}

	return SQLITE_OK;
}

static int dqlite__vfs_open(sqlite3_vfs * vfs,
                            const char *  filename,
                            sqlite3_file *file,
                            int           flags,
                            int *         out_flags) {
	struct dqlite__vfs_root *   root;
	struct dqlite__vfs_file *   f;
	struct dqlite__vfs_content *content;

	int free_slot = -1; /* Index of a free slot in the root->acontent array. */
	int exists    = 0;  /* Whether the file exists already. */

	int type; /* File content type (e.g. database or WAL). */
	int rc;   /* Return code. */

	/* Flags */
	int exclusive = flags & SQLITE_OPEN_EXCLUSIVE;
	int create    = flags & SQLITE_OPEN_CREATE;

	assert(vfs != NULL);
	assert(vfs->pAppData != NULL);
	assert(file != NULL);
	assert(filename != NULL);

	(void)out_flags;

	root = (struct dqlite__vfs_root *)(vfs->pAppData);
	f    = (struct dqlite__vfs_file *)file;

	/* This signals SQLite to not call Close() in case we return an error. */
	f->base.pMethods = 0;

	pthread_mutex_lock(&root->mutex);

	/* Search if the file exists already, and (if it doesn't) if there are
	 * free slots. */
	free_slot = dqlite__vfs_root_content_lookup(root, filename, &content);
	exists    = content != NULL;

	/* If file exists, and the exclusive flag is on, then return an error.
	 *
	 * From sqlite3.h.in:
	 *
	 *   The SQLITE_OPEN_EXCLUSIVE flag is always used in conjunction with
	 *   the SQLITE_OPEN_CREATE flag, which are both directly analogous to
	 *   the O_EXCL and O_CREAT flags of the POSIX open() API.  The
	 *   SQLITE_OPEN_EXCLUSIVE flag, when paired with the
	 *   SQLITE_OPEN_CREATE, is used to indicate that file should always be
	 *   created, and that it is an error if it already exists.  It is not
	 *   used to indicate the file should be opened for exclusive access.
	 */
	assert(!SQLITE_OPEN_EXCLUSIVE || SQLITE_OPEN_CREATE);
	if (exists && exclusive && create) {
		root->error = EEXIST;
		rc          = SQLITE_CANTOPEN;
		goto err;
	}

	if (!exists) {
		/* Check the create flag. */
		if (!create) {
			root->error = ENOENT;
			rc          = SQLITE_CANTOPEN;
			goto err;
		}

		/* This is a new file, so try to create a new entry. */
		if (free_slot == -1) {
			/* No more file content slots available. */
			root->error = ENFILE;
			rc          = SQLITE_CANTOPEN;
			goto err;
		}

		if (flags & SQLITE_OPEN_MAIN_DB) {
			type = DQLITE__FORMAT_DB;
		} else if (flags & SQLITE_OPEN_WAL) {
			type = DQLITE__FORMAT_WAL;
		} else {
			type = DQLITE__FORMAT_OTHER;
		}

		content = sqlite3_malloc(sizeof *content);
		if (content == NULL) {
			root->error = ENOMEM;
			rc          = SQLITE_NOMEM;
			goto err;
		}

		rc = dqlite__vfs_content_init(content, filename, type);
		if (rc != SQLITE_OK) {
			root->error = ENOMEM;
			rc          = SQLITE_NOMEM;
			goto err_after_content_malloc;
		}

		if (type == DQLITE__FORMAT_WAL) {
			/* An associated database file must have been opened. */
			struct dqlite__vfs_content *database;
			rc = dqlite__vfs_root_database_content_lookup(
			    root, filename, &database);
			if (rc != SQLITE_OK) {
				root->error = ENOMEM;
				goto err_after_content_init;
			}
			database->wal = content;
		}

		/* Save the new file content in a free entry of the root file
		 * content array. */
		*(root->contents + free_slot) = content;
	}

	// Populate the new file handle.
	static const sqlite3_io_methods io = {
	    2,                                  // iVersion
	    dqlite__vfs_close,                  // xClose
	    dqlite__vfs_read,                   // xRead
	    dqlite__vfs_write,                  // xWrite
	    dqlite__vfs_truncate,               // xTruncate
	    dqlite__vfs_sync,                   // xSync
	    dqlite__vfs_file_size,              // xFileSize
	    dqlite__vfs_lock,                   // xLock
	    dqlite__vfs_unlock,                 // xUnlock
	    dqlite__vfs_check_reserved_lock,    // xCheckReservedLock
	    dqlite__vfs_file_control,           // xFileControl
	    dqlite__vfs_sector_size,            // xSectorSize
	    dqlite__vfs_device_characteristics, // xDeviceCharacteristics
	    dqlite__vfs_shm_map,                // xShmMap
	    dqlite__vfs_shm_lock,               // xShmLock
	    dqlite__vfs_shm_barrier,            // xShmBarrier
	    dqlite__vfs_shm_unmap,              // xShmUnmap
	    0,
	    0,
	};

	f->base.pMethods = &io;
	f->root          = root;
	f->content       = content;

	content->refcount++;

	pthread_mutex_unlock(&root->mutex);

	return SQLITE_OK;

err_after_content_init:
	dqlite__vfs_content_close(content, 0);

err_after_content_malloc:
	sqlite3_free(content);

err:
	assert(rc != SQLITE_OK);

	pthread_mutex_unlock(&root->mutex);

	return rc;
}

static int dqlite__vfs_delete(sqlite3_vfs *vfs, const char *filename, int dir_sync) {
	struct dqlite__vfs_root *   root;
	struct dqlite__vfs_content *content;
	int                         content_index;
	int                         rc;

	assert(vfs != NULL);
	assert(vfs->pAppData != NULL);

	root = (struct dqlite__vfs_root *)(vfs->pAppData);

	(void)dir_sync;

	pthread_mutex_lock(&root->mutex);

	/* Check if the file exists. */
	content_index = dqlite__vfs_root_content_lookup(root, filename, &content);
	if (content == NULL) {
		root->error = ENOENT;
		rc          = SQLITE_IOERR_DELETE_NOENT;
		goto err;
	}

	/* Check that there are no consumers of this file. */
	if (content->refcount > 0) {
		root->error = EBUSY;
		pthread_mutex_unlock(&root->mutex);
		rc = SQLITE_IOERR_DELETE;
		goto err;
	}

	/* Free all memory allocated for this file. */
	dqlite__vfs_content_close(content, 0);
	sqlite3_free(content);

	/* Reset the file content slot. */
	*(root->contents + content_index) = NULL;

	pthread_mutex_unlock(&root->mutex);

	return SQLITE_OK;

err:
	assert(rc != SQLITE_OK);

	pthread_mutex_unlock(&root->mutex);

	return rc;
}

static int
dqlite__vfs_access(sqlite3_vfs *vfs, const char *filename, int flags, int *result) {
	struct dqlite__vfs_root *   root;
	struct dqlite__vfs_content *content;

	assert(vfs != NULL);
	assert(vfs->pAppData != NULL);

	(void)flags;

	root = (struct dqlite__vfs_root *)(vfs->pAppData);

	pthread_mutex_lock(&root->mutex);

	/* If the file exists, access is always granted. */
	dqlite__vfs_root_content_lookup(root, filename, &content);
	if (content == NULL) {
		root->error = ENOENT;
		*result     = 0;
	} else {
		*result = 1;
	}

	pthread_mutex_unlock(&root->mutex);

	return SQLITE_OK;
}

static int dqlite__vfs_full_pathname(sqlite3_vfs *vfs,
                                     const char * filename,
                                     int          pathname_len,
                                     char *       pathname) {
	(void)vfs;

	// Just return the path unchanged.
	sqlite3_snprintf(pathname_len, pathname, "%s", filename);
	return SQLITE_OK;
}

static void *dqlite__vfs_dl_open(sqlite3_vfs *vfs, const char *filename) {
	(void)vfs;
	(void)filename;

	return 0;
}

static void dqlite__vfs_dl_error(sqlite3_vfs *vfs, int nByte, char *zErrMsg) {
	(void)vfs;

	sqlite3_snprintf(nByte, zErrMsg, "Loadable extensions are not supported");
	zErrMsg[nByte - 1] = '\0';
}

static void (*dqlite__vfs_dl_sym(sqlite3_vfs *vfs, void *pH, const char *z))(void) {
	(void)vfs;
	(void)pH;
	(void)z;

	return 0;
}

static void dqlite__vfs_dl_close(sqlite3_vfs *vfs, void *pHandle) {
	(void)vfs;
	(void)pHandle;

	return;
}

static int dqlite__vfs_randomness(sqlite3_vfs *vfs, int nByte, char *zByte) {
	(void)vfs;
	(void)nByte;
	(void)zByte;

	/* TODO (is this needed?) */
	return SQLITE_OK;
}

static int dqlite__vfs_sleep(sqlite3_vfs *vfs, int microseconds) {
	(void)vfs;

	/* TODO (is this needed?) */
	return microseconds;
}

static int dqlite__vfs_current_time_int64(sqlite3_vfs *vfs, sqlite3_int64 *piNow) {
	static const sqlite3_int64 unixEpoch = 24405875 * (sqlite3_int64)8640000;
	struct timeval             now;

	(void)vfs;

	gettimeofday(&now, 0);
	*piNow = unixEpoch + 1000 * (sqlite3_int64)now.tv_sec + now.tv_usec / 1000;
	return SQLITE_OK;
}

static int dqlite__vfs_current_time(sqlite3_vfs *vfs, double *piNow) {
	// TODO: check if it's always safe to cast a double* to a sqlite3_int64*.
	return dqlite__vfs_current_time_int64(vfs, (sqlite3_int64 *)piNow);
}

static int
dqlite__vfs_get_last_error(sqlite3_vfs *vfs, int NotUsed2, char *NotUsed3) {
	struct dqlite__vfs_root *root = (struct dqlite__vfs_root *)(vfs->pAppData);
	int                      rc;

	(void)vfs;
	(void)NotUsed2;
	(void)NotUsed3;

	pthread_mutex_lock(&root->mutex);
	rc = root->error;
	pthread_mutex_unlock(&root->mutex);

	return rc;
}

int dqlite_vfs_register(const char *name, sqlite3_vfs **out) {
	sqlite3_vfs *            vfs;
	char *                   zName;
	struct dqlite__vfs_root *root;
	int                      rc;

	assert(name != NULL);
	assert(out != NULL);

	vfs = sqlite3_vfs_find(name);
	if (vfs != NULL) {
		rc = SQLITE_ERROR;
		goto err;
	}

	vfs = sqlite3_malloc(sizeof *vfs);
	if (vfs == NULL) {
		rc = SQLITE_NOMEM;
		goto err;
	}

	root = sqlite3_malloc(sizeof *root);
	if (root == NULL) {
		rc = SQLITE_NOMEM;
		goto err_after_vfs_malloc;
	}

	rc = dqlite__vfs_root_init(root);
	if (rc != 0) {
		assert(rc == SQLITE_NOMEM);
		goto err_after_root_malloc;
	}

	zName = sqlite3_malloc(strlen(name) + 1);
	if (zName == NULL) {
		rc = SQLITE_NOMEM;
		goto err_after_root_init;
	}

	/* Make a copy of the provided name, so clients can free the string if
	 * they need. */
	strcpy(zName, name);

	vfs->iVersion   = 2;
	vfs->szOsFile   = sizeof(struct dqlite__vfs_file);
	vfs->mxPathname = DQLITE__VFS_MAX_PATHNAME;
	vfs->pNext      = 0;
	vfs->zName      = zName;
	vfs->pAppData   = root;
	vfs->xOpen      = dqlite__vfs_open;
	vfs->xDelete    = dqlite__vfs_delete;
	vfs->xAccess    = dqlite__vfs_access;

	vfs->xFullPathname = dqlite__vfs_full_pathname;

	vfs->xDlOpen  = dqlite__vfs_dl_open;
	vfs->xDlError = dqlite__vfs_dl_error;
	vfs->xDlSym   = dqlite__vfs_dl_sym;
	vfs->xDlClose = dqlite__vfs_dl_close;

	vfs->xRandomness = dqlite__vfs_randomness;
	vfs->xSleep      = dqlite__vfs_sleep;

	vfs->xCurrentTime      = dqlite__vfs_current_time;
	vfs->xGetLastError     = dqlite__vfs_get_last_error;
	vfs->xCurrentTimeInt64 = dqlite__vfs_current_time_int64;

	sqlite3_vfs_register(vfs, 0);

	*out = vfs;

	return SQLITE_OK;

err_after_root_init:
	dqlite__vfs_root_close(root);

err_after_root_malloc:
	sqlite3_free(root);

err_after_vfs_malloc:
	sqlite3_free(vfs);

err:
	assert(rc != SQLITE_OK);
	*out = NULL;

	return rc;
}

void dqlite_vfs_unregister(sqlite3_vfs *vfs) {
	struct dqlite__vfs_root *root;

	assert(vfs != NULL);

	sqlite3_vfs_unregister(vfs);

	root = (struct dqlite__vfs_root *)(vfs->pAppData);

	dqlite__vfs_root_close(root);

	sqlite3_free(root);
	sqlite3_free((char *)vfs->zName);
	sqlite3_free(vfs);
}

/* Guess the file type by looking the filename. */
static int dqlite__vfs_guess_file_type(const char *filename) {
	/* TODO: improve the check. */
	if (strstr(filename, "-wal") != NULL) {
		return DQLITE__FORMAT_WAL;
	}

	return DQLITE__FORMAT_DB;
}

int dqlite_vfs_snapshot(sqlite3_vfs *vfs,
                        const char * filename,
                        uint8_t **   buf,
                        size_t *     len) {
	int           type;
	int           flags;
	sqlite3_file *file;
	unsigned      page_size;
	sqlite3_int64 offset = 0;
	int           rc;

	assert(vfs != NULL);
	assert(buf != NULL);

	type = dqlite__vfs_guess_file_type(filename);

	/* Common flags */
	flags = SQLITE_OPEN_READWRITE;

	if (type == DQLITE__FORMAT_DB) {
		flags |= SQLITE_OPEN_MAIN_DB;
	} else {
		flags |= SQLITE_OPEN_WAL;
	}

	/* Open the file */
	file = (sqlite3_file *)sqlite3_malloc(sizeof(*file));
	if (file == NULL) {
		rc = SQLITE_NOMEM;
		goto err;
	}

	rc = dqlite__vfs_open(vfs, filename, file, flags, &flags);
	if (rc != SQLITE_OK) {
		goto err_after_file_malloc;
	}

	/* Get the file size */
	rc = file->pMethods->xFileSize(file, (sqlite3_int64 *)len);
	if (rc != SQLITE_OK) {
		goto err_after_file_open;
	}

	/* Allocate the read buffer */
	*buf = sqlite3_malloc(*len);
	if (*buf == NULL) {
		rc = SQLITE_NOMEM;
		goto err_after_file_open;
	}

	/* Read the header. The buffer size is enough for both cases. */
	rc = file->pMethods->xRead(file, *buf, DQLITE__FORMAT_WAL_HDR_SIZE, 0);
	if (rc != SQLITE_OK) {
		goto err_after_buf_malloc;
	}

	/* Figure the page size */
	rc = dqlite__format_get_page_size(type, *buf, &page_size);
	if (rc != SQLITE_OK) {
		goto err_after_buf_malloc;
	}

	/* If this is a WAL file, we have already read the header and we can
	 * move on. */
	if (type == DQLITE__FORMAT_WAL) {
		offset += DQLITE__FORMAT_WAL_HDR_SIZE;
	}

	while ((size_t)offset < *len) {
		uint8_t *pos = (*buf) + offset;

		if (type == DQLITE__FORMAT_WAL) {
			/* Read the frame header */
			rc = file->pMethods->xRead(
			    file, pos, DQLITE__FORMAT_WAL_FRAME_HDR_SIZE, offset);
			if (rc != SQLITE_OK) {
				goto err_after_buf_malloc;
			}
			offset += DQLITE__FORMAT_WAL_FRAME_HDR_SIZE;
			pos += DQLITE__FORMAT_WAL_FRAME_HDR_SIZE;
		}

		/* Read the page */
		rc = file->pMethods->xRead(file, pos, page_size, offset);
		if (rc != SQLITE_OK) {
			goto err_after_buf_malloc;
		}
		offset += page_size;
	};

	file->pMethods->xClose(file);
	sqlite3_free(file);

	return SQLITE_OK;

err_after_buf_malloc:
	sqlite3_free(*buf);

err_after_file_open:
	file->pMethods->xClose(file);

err_after_file_malloc:
	sqlite3_free(file);

err:
	assert(rc != SQLITE_OK);

	*buf = NULL;
	*len = 0;

	return rc;
}

int dqlite_vfs_restore(sqlite3_vfs *vfs,
                       const char * filename,
                       uint8_t *    buf,
                       size_t       len) {
	int err = SQLITE_OK;

	sqlite3_file *file;
	int           exists;
	int           flags;
	unsigned int  page_size;
	sqlite3_int64 offset;
	uint8_t *     cursor;

	assert(vfs != NULL);
	assert(buf != NULL);
	assert(len > 0);

	/* First check if the file exists */
	err = dqlite__vfs_access(vfs, filename, 0, &exists);
	if (err != SQLITE_OK) {
		return err;
	}

	/* If it exists, remove it. */
	if (exists) {
		err = dqlite__vfs_delete(vfs, filename, 0);
		if (err != SQLITE_OK) {
			return err;
		}
	}

	/* Common flags */
	flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;

	/* Determine if this is a database or a WAL file
	 *
	 * TODO: improve the check. */
	if (strstr(filename, "-wal") != NULL) {
		flags |= SQLITE_OPEN_WAL;
	} else {
		flags |= SQLITE_OPEN_MAIN_DB;
	}

	/* Create the file */
	file = (sqlite3_file *)sqlite3_malloc(sizeof(*file));
	if (file == NULL) {
		err = SQLITE_NOMEM;
		goto err_file_malloc;
	}

	err = dqlite__vfs_open(vfs, filename, file, flags, &flags);
	if (err != SQLITE_OK) {
		goto err_open;
	}

	if (flags & SQLITE_OPEN_MAIN_DB) {
		/* Write a database file */
		cursor = buf;

		err = dqlite__format_get_page_size(
		    DQLITE__FORMAT_DB, cursor, &page_size);
		if (err != SQLITE_OK) {
			goto err_write;
		}

		assert((len % page_size) == 0);

		for (offset = 0; offset < (sqlite3_int64)(len);
		     offset += page_size) {
			cursor = buf + offset;
			err    = dqlite__vfs_write(file, cursor, page_size, offset);
			if (err != SQLITE_OK) {
				goto err_write;
			}
		}
	} else {
		/* Write a WAL file */
		int frame_size;
		int rc;

		rc = dqlite__format_get_page_size(
		    DQLITE__FORMAT_WAL, buf, &page_size);
		if (rc != SQLITE_OK) {
			return rc;
		}

		frame_size = DQLITE__FORMAT_WAL_FRAME_HDR_SIZE + page_size;
		assert(((len - DQLITE__FORMAT_WAL_HDR_SIZE) % frame_size) == 0);

		cursor = buf;
		err =
		    dqlite__vfs_write(file, cursor, DQLITE__FORMAT_WAL_HDR_SIZE, 0);
		if (err != SQLITE_OK) {
			goto err_write;
		}

		for (offset = DQLITE__FORMAT_WAL_HDR_SIZE;
		     offset < (sqlite3_int64)(len);
		     offset += frame_size) {
			cursor = buf + offset;
			err    = dqlite__vfs_write(
                            file, cursor, DQLITE__FORMAT_WAL_FRAME_HDR_SIZE, offset);
			if (err != SQLITE_OK) {
				goto err_write;
			}

			cursor = buf + offset + DQLITE__FORMAT_WAL_FRAME_HDR_SIZE;
			err    = dqlite__vfs_write(
                            file,
                            cursor,
                            page_size,
                            offset + DQLITE__FORMAT_WAL_FRAME_HDR_SIZE);
			if (err != SQLITE_OK) {
				goto err_write;
			}
		}
	}

err_write:
	dqlite__vfs_close(file);

err_open:
	sqlite3_free(file);

err_file_malloc:

	return err;
}
