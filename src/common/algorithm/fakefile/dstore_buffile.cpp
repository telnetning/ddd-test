/*
 * Copyright (C) 2026 Huawei Technologies Co.,Ltd.
 *
 * dstore is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * dstore is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. if not, see <https://www.gnu.org/licenses/>.
 *
 * Description:
 *      Management of large buffered files, primarily temporary files.
 */

#include "common/algorithm/fakefile/dstore_buffile.h"
#include "common/algorithm/fakefile/dstore_fakefile.h"
#include "transaction/dstore_resowner.h"
#include "securec.h"

namespace DSTORE {

/*
 * We break BufFiles into gigabyte-sized segments, regardless of RELSEG_SIZE.
 * The reason is that we'd like large temporary BufFiles to be spread across
 * multiple tablespaces when available.
 */
static constexpr off_t MAX_PHYSICAL_FILESIZE = 0x40000000;
#define BUFFILE_SEG_SIZE (MAX_PHYSICAL_FILESIZE / BLCKSZ)

/*
 * This data structure represents a buffered file that consists of one
 * or more physical files (each accessed through a virtual file descriptor
 * managed by fd.c).
 */
struct BufFile {
    int numFiles; /* number of physical file in set */
    int seqNum = 0; /* seq number of the buffer file */
    /* all files except the last have length exactly MAX_PHYSICAL_FILESIZE */
    int* files;    /* palloc'd array with numFiles entries */
    off_t* offsets; /* palloc'd array with numFiles entries */

    /*
     * offsets[i] is the current seek position of files[i].  We use this
     * to avoid making redundant FileSeek calls.
     */
    bool isTemp;      /* can only add files if this is TRUE */
    bool isInterXact; /* keep open over transactions? */
    bool dirty;       /* does buffer need to be written ? */

    bool readOnly;            /* has the file been set to read only? */
    const char *name;         /* name of this buffile if shared */

    /*
     * resowner is the ResourceOwner to use for underlying temp files.  (We
     * don't need to remember the memory context we're using explicitly,
     * because after creation we only repalloc our arrays larger)
     */
    ResourceOwner resowner;

    /*
     * "current pos" is position of start of buffer within the logical file.
     * The position as seen by user of BufFile is (curFile, curOffset + pos).
     */
    int curFile;     /* file index (0..n) part of current pos */
    off_t curOffset; /* offset part of current pos */
    int pos;         /* next read or write position in buffer */
    int nbytes;      /* total # of valid bytes in buffer */
    char* buffer;    /* adio need pointer align */

    char pad; /* extra 1 byte, just a workaround for the memory issue of pread */

    void GenTempFileName(char* path, size_t maxPathLen = MAXPGPATH);
    int OpenTemporaryFile();
    void CloseTemporaryFile();
};

static BufFile* MakeBufFile(const char *tmpFileNameBase);
static RetStatus ExtendBufFile(BufFile* file);
static void BufFileLoadBuffer(BufFile* file);
static void BufFileDumpBuffer(BufFile* file);
static int BufFileFlush(BufFile* file);

/*
 * Return the path of the temp directory for current sql number.
 * path's length must bigger than MAXPGPATH.
 */
void BufFile::GenTempFileName(char *path, size_t maxPathLen)
{
    StorageAssert(seqNum > 0);
    StorageAssert(name != nullptr);

    int err_rc = snprintf_s(path, maxPathLen, maxPathLen - 1, "%s%d", name, seqNum);
    storage_securec_check_ss(err_rc);
}

/* open a new file and increase the seqNum */
int BufFile::OpenTemporaryFile()
{
    seqNum++; /* temp file seq number start from 1 */
    char tmpFilePath[MAXPGPATH] = {0};
    GenTempFileName(tmpFilePath);
    /*
     * Open the file.  Note: we don't use O_EXCL, in case there is an orphaned
     * temp file that can be reused.
     */
    constexpr int defaultFileMode = 0600;
    int file = FileOpen(tmpFilePath, O_RDWR | O_CREAT | O_TRUNC, defaultFileMode);
    if (!IsFdValid(file)) {
        storage_set_error(BUFFILE_ERROR_FAIL_CREATE_TEMP_FILE);
    }
    return file;
}

/* close and remove latest file and decrease the seqNum */
void BufFile::CloseTemporaryFile()
{
    if (unlikely(files == nullptr || numFiles <= 0)) {
        return;
    }
    FileClose(files[numFiles - 1]);

    char tmpFilePath[MAXPGPATH] = {0};
    GenTempFileName(tmpFilePath);
    FileUnlink(tmpFilePath);

    seqNum--;
}

static void ReleaseBuffileRes(BufFile* file)
{
    DstorePfreeExt(file->files);
    DstorePfreeExt(file->offsets);
    DstorePfreeExt(file);
}

/*
 * Create a BufFile given the first underlying physical file.
 * NOTE: caller must set isTemp and isInterXact if appropriate.
 * return nullptr if failed, and set storage_errno.
 * !!caller must make sure tmpFileNameBase will not be freed during the BufFile's lifetime.
 */
static BufFile* MakeBufFile(const char *tmpFileNameBase)
{
    BufFile* file = nullptr;
    /*
     * In ADIO scene, the pointer file->buffer must BLCKSZ byte align, so we need to palloc another BLOCK.
     * AlignMemoryContext will be reset when the transaction aborts, we should alloc the buffile in
     * g_dstoreCurrentMemoryContext rather than AlignMemoryContext, because the buffile may live in different
     * transactions.
     */
    {
        file = static_cast<BufFile *>(DstorePalloc0(sizeof(BufFile) + BLCKSZ));
        if (file == nullptr) {
            return nullptr;
        }
        file->buffer = static_cast<char*>(static_cast<void*>(file)) + sizeof(BufFile);
    }

    file->seqNum = 0;
    file->name = tmpFileNameBase;
    file->numFiles = 1;
    file->files = static_cast<int *>(DstorePalloc(sizeof(int)));
    file->files[0] = file->OpenTemporaryFile();
    file->offsets = static_cast<off_t *>(DstorePalloc(sizeof(off_t)));
    if (unlikely((!IsFdValid(file->files[0])) || file->files == nullptr || file->offsets == nullptr)) {
        /* storage_set_error is called when file open and memory allocated */
        ReleaseBuffileRes(file);
        return nullptr;
    }
    file->offsets[0] = 0L;
    file->isTemp = false;
    file->isInterXact = false;
    file->dirty = false;
    file->resowner = nullptr;
    file->curFile = 0;
    file->curOffset = 0L;
    file->nbytes = 0;
    file->pos = 0;
    file->pad = '\0';
    file->readOnly = false;

    return file;
}

/*
 * Add another component temp file.
 */
static RetStatus ExtendBufFile(BufFile* file)
{
    int pfile;

    StorageAssert(file->isTemp);
    pfile = file->OpenTemporaryFile();

    int *tmpfiles = (int *)DstoreRepalloc(file->files, static_cast<uint>(file->numFiles + 1) * sizeof(int));
    if (STORAGE_VAR_NULL(tmpfiles)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("[%s]Failed to allocate memory for tmpfiles.", __FUNCTION__));
        return DSTORE_FAIL;
    }
    off_t *tmpoffsets = (off_t *)DstoreRepalloc(file->offsets, static_cast<uint>(file->numFiles + 1) * sizeof(off_t));
    if (STORAGE_VAR_NULL(tmpoffsets)) {
        DstorePfreeExt(tmpfiles);
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("[%s]Failed to allocate memory for tmpoffsets.", __FUNCTION__));
        return DSTORE_FAIL;
    }
    file->files = tmpfiles;
    file->offsets = tmpoffsets;
    file->files[file->numFiles] = pfile;
    file->offsets[file->numFiles] = 0L;
    file->numFiles++;
    return DSTORE_SUCC;
}

/*
 * Create a BufFile for a new temporary file (which will expand to become multiple
 * temporary files if more than MAX_PHYSICAL_FILESIZE bytes are written to it).
 *
 * If interXact is true, the temp file will not be automatically deleted at end of transaction.
 *
 * Note: if interXact is true, the caller had better be calling us in a memory context,
 * and with a resource owner, that will survive across transaction boundaries.
 *
 * return nullptr if failed, and set storage_errno.
 */
static BufFile* CreateTempBufFile(bool interXact, const char* baseTmpFileName)
{
    BufFile* file = MakeBufFile(baseTmpFileName);
    if (file) {
        file->isTemp = true;
        file->isInterXact = interXact;
    }

    return file;
}

/*
 * !!caller must make sure tmpFileNameBase will not be freed during the BufFile's lifetime.
 */
BufFile *BufFileCreateTemp(bool interXact, const char *baseTmpFileName)
{
    StorageReleasePanic(baseTmpFileName == nullptr, MODULE_COMMON, ErrMsg("base temp file name is nullptr."));
    StorageReleasePanic(strlen(baseTmpFileName) == 0, MODULE_COMMON, ErrMsg("base temp file name is empty."));
    /* create main temp buffer file */
    BufFile *mainBufFile = CreateTempBufFile(interXact, baseTmpFileName);
    return mainBufFile;
}

/* Like fclose(), this also implicitly FileCloses the underlying File. */
static void CloseTempBufFile(BufFile* file)
{
    /* flush any unwritten data */
    (void)BufFileFlush(file);
    /* close the underlying file(s) (with delete if it is a temp file) */
    while (file->numFiles >= 1) {
        file->CloseTemporaryFile();
        file->numFiles--;
    }

    /* release the buffer space */
    ReleaseBuffileRes(file);
}

void BufFileClose(BufFile* file) noexcept
{
    if (file) {
        /* close main temp buf file */
        CloseTempBufFile(file);
    }
}

/*
 * Load some data into buffer, if possible, starting from curOffset.
 * At call, must have dirty = false, pos and nbytes = 0.
 * On exit, nbytes is the number of bytes loaded.
 */
static void BufFileLoadBuffer(BufFile* file)
{
    int thisfile;

    /*
     * Advance to next component file if necessary and possible.
     *
     * This path can only be taken if there is more than one component, so it
     * won't interfere with reading a non-temp file that is over MAX_PHYSICAL_FILESIZE.
     */
    if (file->curOffset >= MAX_PHYSICAL_FILESIZE && file->curFile + 1 < file->numFiles) {
        file->curFile++;
        file->curOffset = 0L;
    }

    /* May need to reposition physical file. */
    thisfile = file->files[file->curFile];
    if (file->curOffset != file->offsets[file->curFile]) {
        file->offsets[file->curFile] = file->curOffset;
    }

    /* Read whatever we can get, up to a full bufferload. */
    file->nbytes = FilePRead(thisfile, file->buffer, BLCKSZ, file->curOffset);

    if (file->nbytes < 0) {
        file->nbytes = 0;
    }

    file->offsets[file->curFile] += file->nbytes;
}

/*
 * Dump buffer contents starting at curOffset.
 * At call, should have dirty = true, nbytes > 0.
 * On exit, if successful write, dirty is cleared and curOffset is advanced.
 */
static void BufFileDumpBuffer(BufFile* file)
{
    int wpos = 0;
    int bytestowrite;
    int thisfile;

    /*
     * Unlike BufFileLoadBuffer, we have to dump the whole buffer even if it
     * crosses a component-file boundary; so we need a loop.
     */
    while (wpos < file->nbytes) {
        /* Advance to next component file if necessary and possible. */
        if (file->curOffset >= MAX_PHYSICAL_FILESIZE && file->isTemp) {
            while (file->curFile + 1 >= file->numFiles) {
                ExtendBufFile(file);
            }
            file->curOffset = 0L;
            file->curFile++;
        }

        /*
         * Enforce per-file size limit only for temp files, else just try to
         * write as much as asked
         */
        bytestowrite = file->nbytes - wpos;
        if (file->isTemp) {
            off_t availbytes = MAX_PHYSICAL_FILESIZE - file->curOffset;

            if (static_cast<off_t>(bytestowrite) > availbytes) {
                bytestowrite = static_cast<int>(availbytes);
            }
        }

        /*
         * May need to reposition physical file.
         */
        thisfile = file->files[file->curFile];
        if (file->curOffset != file->offsets[file->curFile]) {
            file->offsets[file->curFile] = file->curOffset;
        }
        bytestowrite =
            FilePWrite(thisfile, file->buffer + wpos, bytestowrite, file->curOffset);
        if (bytestowrite <= 0) {
            return; /* failed to write */
        }

        file->offsets[file->curFile] += bytestowrite;
        file->curOffset += bytestowrite;
        wpos += bytestowrite;
    }
    file->dirty = false;

    /*
     * At this point, curOffset has been advanced to the end of the buffer,
     * ie, its original value + nbytes.  We need to make it point to the logical
     * file position, ie, original value + pos, in case that is less (as could
     * happen due to a small backwards seek in a dirty buffer!)
     */
    file->curOffset -= (file->nbytes - file->pos);
    if (file->curOffset < 0) { /* handle possible segment crossing */
        file->curFile--;
        StorageAssert(file->curFile >= 0);
        file->curOffset += MAX_PHYSICAL_FILESIZE;
    }

    /* Now we can set the buffer empty without changing the logical position */
    file->pos = 0;
    file->nbytes = 0;
}

/*
 * Like fread() except we assume 1-byte element size.
 * NOTE: We change return value(0->EOF) when BufFileFlush failed,
 * because some caller can not distinguish normal return or error return.
 * and in this case, param size can not set to (size_t)(-1), because
 * we treat it as error condition.
 */
size_t BufFileRead(BufFile* file, void* ptr, size_t size)
{
    size_t nthistime;
    size_t nread = 0;

    if (file->dirty) {
        if (BufFileFlush(file) != 0) {
            return static_cast<size_t>(EOF); /* could not flush... */
        }
        StorageAssert(!file->dirty);
    }

    while (size > 0) {
        if (file->pos >= file->nbytes) {
            /* try to load more data into buffer */
            file->curOffset += file->pos;
            file->nbytes = 0;
            file->pos = 0;
            BufFileLoadBuffer(file);
            if (file->nbytes <= 0) {
                break; /* no more data available */
            }
        }

        nthistime = static_cast<size_t>(static_cast<unsigned int>(file->nbytes - file->pos));
        if (nthistime > size) {
            nthistime = size;
        }
        StorageAssert(nthistime > 0);

        errno_t rc = memcpy_s(ptr, nthistime, file->buffer + file->pos, nthistime);
        storage_securec_check(rc, "\0", "\0");

        file->pos += static_cast<int>(nthistime);
        ptr = static_cast<void*>(static_cast<char*>(ptr) + nthistime);
        size -= nthistime;
        nread += nthistime;
    }

    return nread;
}

/*
 * BufFileWrite
 *
 * Like fwrite() except we assume 1-byte element size.
 */
RetStatus BufFileWrite(BufFile* file, void* ptr, size_t size)
{
    size_t nthistime;
    errno_t rc = EOK;

    StorageAssert(!file->readOnly);

    while (size > 0) {
        if (file->pos >= BLCKSZ) {
            /* Buffer is full, dump it out */
            if (file->dirty) {
                BufFileDumpBuffer(file);
                if (file->dirty) {
                    /* I/O error */
                    char tmpFileName[MAXPGPATH] = {0};
                    GetFileName(file, tmpFileName);
                    storage_set_error(BUFFILE_ERROR_FAIL_WRITE_TEMP_FILE, tmpFileName);
                    ErrLog(DSTORE_ERROR, MODULE_COMMON,
                        ErrMsg("[%s]Failed to write file \"%s\"", __FUNCTION__, tmpFileName));
                    return DSTORE_FAIL;
                }
            } else {
                /* went directly from reading to writing? */
                file->curOffset += file->pos;
                file->pos = 0;
                file->nbytes = 0;
            }
        }

        nthistime = static_cast<size_t>(static_cast<unsigned int>(BLCKSZ - file->pos));
        if (nthistime > size) {
            nthistime = size;
        }
        StorageAssert(nthistime > 0);

        rc = memcpy_s(file->buffer + file->pos, nthistime, ptr, nthistime);
        storage_securec_check(rc, "", "");
#ifdef MEMORY_CONTEXT_CHECKING
        DstoreAllocSetCheckPointer(file);
#endif

        file->dirty = true;
        file->pos += static_cast<int>(nthistime);
        if (file->nbytes < file->pos) {
            file->nbytes = file->pos;
        }
        ptr = static_cast<void*>(static_cast<char*>(ptr) + nthistime);
        size -= nthistime;
    }
    return DSTORE_SUCC;
}

/* Like fflush() */
static int BufFileFlush(BufFile* file)
{
    if (!file->dirty) {
        return 0;
    }

    BufFileDumpBuffer(file);
    if (file->dirty) {
        return EOF;
    }

    return 0;
}

/*
 * Like fseek(), except that target position needs two values in order to
 * work when logical filesize exceeds maximum value representable by long.
 * We do not support relative seeks across more than LONG_MAX, however.
 *
 * Result is 0 if OK, EOF if not.  Logical position is not moved if
 * an impossible seek is attempted.
 */
int BufFileSeek(BufFile* file, int fileno, off_t offset, int whence)
{
    int new_file;
    off_t new_offset;

    switch (whence) {
        case SEEK_SET: {
            if (fileno < 0) {
                return EOF;
            }
            new_offset = offset;
            new_file = fileno;
            break;
        }
        case SEEK_CUR: {
            /*
             * Relative seek considers only the signed offset, ignoring fileno.
             * Note that large offsets (> 1 gig) risk overflow in this add,
             * unless we have 64-bit off_t.
             */
            new_file = file->curFile;
            new_offset = (file->curOffset + file->pos) + offset;
            break;
        }
#ifdef NOT_USED
        case SEEK_END: {
            /* could be implemented, but not needed currently */
            break;
        }
#endif
        default: {
            return EOF;
        }
    }
    while (new_offset < 0) {
        new_file--;
        if (new_file < 0) {
            return EOF;
        }
        new_offset += MAX_PHYSICAL_FILESIZE;
    }
    if (new_file == file->curFile && new_offset >= file->curOffset && new_offset <= file->curOffset + file->nbytes) {
        /*
         * Seek is to a point within existing buffer; we can just adjust
         * pos-within-buffer, without flushing buffer.  Note this is OK whether
         * reading or writing, but buffer remains dirty if we were writing.
         */
        file->pos = static_cast<int>(new_offset - file->curOffset);
        return 0;
    }
    /* Otherwise, have to reposition buffer, so flush any dirty data */
    if (BufFileFlush(file) != 0) {
        return EOF;
    }

    /*
     * At this point and no sooner, check for seek past last segment. The
     * above flush could have created a new segment, so checking sooner
     * would not work (at least not with this code).
     */
    if (file->isTemp) {
        /* convert seek to "start of next seg" to "end of last seg" */
        if (new_file == file->numFiles && new_offset == 0) {
            new_offset = MAX_PHYSICAL_FILESIZE;
            new_file--;
        }
        for (; new_offset > MAX_PHYSICAL_FILESIZE; new_offset -= MAX_PHYSICAL_FILESIZE) {
            if (++new_file >= file->numFiles) {
                return EOF;
            }
        }
    }
    if (new_file >= file->numFiles) {
        return EOF;
    }
    /* Seek is OK */
    file->curFile = new_file;
    file->curOffset = new_offset;
    file->nbytes = 0;
    file->pos = 0;

    return 0;
}

void BufFileTell(BufFile* file, int* fileno, off_t* offset)
{
    *fileno = file->curFile;
    *offset = file->curOffset + file->pos;
}

/*
 * Performs absolute seek to the start of the n'th BLCKSZ-sized block of
 * the file.  Note that users of this interface will fail if their files
 * exceed BLCKSZ * LONG_MAX bytes, but that is quite a lot; we don't work
 * with tables bigger than that, either...
 *
 * Result is 0 if OK, EOF if not. Logical position is not moved if an impossible
 * seek is attempted.
 */
int BufFileSeekBlock(BufFile* file, long blknum)
{
    return BufFileSeek(file, static_cast<int>(blknum / BUFFILE_SEG_SIZE),
                       static_cast<off_t>(blknum % BUFFILE_SEG_SIZE) * BLCKSZ, SEEK_SET);
}

/*
 * Return the current shared BufFile size.
 *
 * Counts any holes left behind by BufFileAppend as part of the size.
 * ereport()s on failure.
 */
int64 BufFileSize(BufFile *file)
{
    int64 lastFileSize;

    /* Get the size of the last physical file by seeking to end. */
    lastFileSize = FileSeek(file->files[file->numFiles - 1], 0, SEEK_END);
    if (lastFileSize < 0) {
        (void)printf("could not determine size of temporary file :%s\n ", file->name);
    }
    file->offsets[file->numFiles - 1] = lastFileSize;

    return ((file->numFiles - 1) * static_cast<int64>(MAX_PHYSICAL_FILESIZE)) + lastFileSize;
}

/*
 * Append the contents of source file (managed within shared fileset) to
 * end of target file (managed within same shared fileset).
 *
 * Note that operation subsumes ownership of underlying resources from "source".
 * Caller should never call BufFileClose against source having called here first.
 * Resource owners for source and target must match, too.
 *
 * This operation works by manipulating lists of segment files, so the
 * file content is always appended at a MAX_PHYSICAL_FILESIZE-aligned boundary,
 * typically creating empty holes before the boundary. These areas do not
 * contain any interesting data, and cannot be read from by caller.
 *
 * Returns the block number within target where the contents of source begins.
 * Caller should apply this as an offset when working off block positions that
 * are in terms of the original BufFile space.
 */
long BufFileAppend(BufFile *target, BufFile *source)
{
    long startBlock = target->numFiles * BUFFILE_SEG_SIZE;
    int newNumFiles = target->numFiles + source->numFiles;

    StorageAssert(source->readOnly);
    StorageAssert(!source->dirty);

    int *tmp_file = static_cast<int *>(DstoreRepalloc(target->files, sizeof(int) * static_cast<uint>(newNumFiles)));
    off_t *tmp_offset =
        static_cast<off_t *>(DstoreRepalloc(target->offsets, sizeof(off_t) * static_cast<uint>(newNumFiles)));
    /* If - 1 is returned, the execution fails */
    if (STORAGE_VAR_NULL(tmp_file) || STORAGE_VAR_NULL(tmp_offset)) {
        return -1;
    }
    target->files = tmp_file;
    target->offsets = tmp_offset;
    for (int i = target->numFiles; i < newNumFiles; i++) {
        target->files[i] = source->files[i - target->numFiles];
        target->offsets[i] = source->offsets[i - target->numFiles];
    }
    target->numFiles = newNumFiles;

    return startBlock;
}

bool IsBufFileDirty(BufFile *file)
{
    return file->dirty;
}

void GetFileName(BufFile *file, char *fileName)
{
    if (STORAGE_VAR_NULL(file) || STORAGE_VAR_NULL(fileName)) {
        return;
    }
    file->GenTempFileName(fileName);
}

}
