// ============================================================================
// fs.c - user FileSytem API
// ============================================================================

#include "bfs.h"
#include "fs.h"

// ============================================================================
// Close the file currently open on file descriptor 'fd'.
// ============================================================================
i32 fsClose(i32 fd) {
    i32 inum = bfsFdToInum(fd);
    bfsDerefOFT(inum);
    return 0;
}


// ============================================================================
// Create the file called 'fname'.  Overwrite, if it already exsists.
// On success, return its file descriptor.  On failure, EFNF
// ============================================================================
i32 fsCreate(str fname) {
    i32 inum = bfsCreateFile(fname);
    if (inum == EFNF) return EFNF;
    return bfsInumToFd(inum);
}


// ============================================================================
// Format the BFS disk by initializing the SuperBlock, Inodes, Directory and 
// Freelist.  On succes, return 0.  On failure, abort
// ============================================================================
i32 fsFormat() {
    FILE *fp = fopen(BFSDISK, "w+b");
    if (fp == NULL) FATAL(EDISKCREATE);

    i32 ret = bfsInitSuper(fp);               // initialize Super block
    if (ret != 0) {
        fclose(fp);
        FATAL(ret);
    }

    ret = bfsInitInodes(fp);                  // initialize Inodes block
    if (ret != 0) {
        fclose(fp);
        FATAL(ret);
    }

    ret = bfsInitDir(fp);                     // initialize Dir block
    if (ret != 0) {
        fclose(fp);
        FATAL(ret);
    }

    ret = bfsInitFreeList();                  // initialize Freelist
    if (ret != 0) {
        fclose(fp);
        FATAL(ret);
    }

    fclose(fp);
    return 0;
}


// ============================================================================
// Mount the BFS disk.  It must already exist
// ============================================================================
i32 fsMount() {
    FILE *fp = fopen(BFSDISK, "rb");
    if (fp == NULL) FATAL(ENODISK);           // BFSDISK not found
    fclose(fp);
    return 0;
}


// ============================================================================
// Open the existing file called 'fname'.  On success, return its file 
// descriptor.  On failure, return EFNF
// ============================================================================
i32 fsOpen(str fname) {
    i32 inum = bfsLookupFile(fname);        // lookup 'fname' in Directory
    if (inum == EFNF) return EFNF;
    return bfsInumToFd(inum);
}

// ============================================================================
// Read 'numb' bytes of data from the cursor in the file currently fsOpen'd on
// File Descriptor 'fd' into 'buf'.  On success, return actual number of bytes
// read (may be less than 'numb' if we hit EOF).  On failure, abort
// ============================================================================
i32 fsRead(i32 fd, i32 numb, void *buf) {
    if (numb <= 0) FATAL(ENEGNUMB);

    i32 inum = bfsFdToInum(fd);      // Convert file descriptor to inode number
    i32 cursor = bfsTell(fd);        // Get curr cursor position
    i32 size = bfsGetSize(inum);     // Get file size

    // Calculate max bytes to read
    i32 bytesToRead = (cursor + numb > size) ? (size - cursor) : numb;
    if (bytesToRead <= 0) return 0;  // End of file / nothing to read

    i8 *buf8 = (i8 *) buf;
    i32 bytesRead = 0;

    // Calculate starting file block number (fbn) and offset within that block
    i32 fbn = cursor / BYTESPERBLOCK;
    i32 offset = cursor % BYTESPERBLOCK;

    // Read block by block
    while (bytesRead < bytesToRead) {
        i8 blockBuf[BYTESPERBLOCK] = {0};   // Temp buffer for block data
        i32 dbn = bfsFbnToDbn(inum, fbn);   // Convert file block to disk block

        // If block exists, read it; otherwise leave as zeroed
        if (dbn != ENODBN) {
            bioRead(dbn, blockBuf);
        }

        // Calculate bytes to read from this block
        i32 blockBytesToRead = BYTESPERBLOCK - offset;
        if (bytesRead + blockBytesToRead > bytesToRead) {
            blockBytesToRead = bytesToRead - bytesRead;
        }

        // Copy data from block buffer to output buffer
        memcpy(buf8 + bytesRead, blockBuf + offset, blockBytesToRead);

        bytesRead += blockBytesToRead;
        offset = 0;                  // Reset offset for next blocks
        fbn++;                       // Move to next block
    }

    // Update cursor position
    bfsSetCursor(inum, cursor + bytesRead);

    return bytesRead;   // Actual num of bytes read
}

// ============================================================================
// Move the cursor for the file currently open on File Descriptor 'fd' to the
// byte-offset 'offset'.  'whence' can be any of:
//
//  SEEK_SET : set cursor to 'offset'
//  SEEK_CUR : add 'offset' to the current cursor
//  SEEK_END : add 'offset' to the size of the file
//
// On success, return 0.  On failure, abort
// ============================================================================
i32 fsSeek(i32 fd, i32 offset, i32 whence) {

    if (offset < 0) FATAL(EBADCURS);

    i32 inum = bfsFdToInum(fd);
    i32 ofte = bfsFindOFTE(inum);

    switch (whence) {
        case SEEK_SET:
            g_oft[ofte].curs = offset;
            break;
        case SEEK_CUR:
            g_oft[ofte].curs += offset;
            break;
        case SEEK_END: {
            i32 end = fsSize(fd);
            g_oft[ofte].curs = end + offset;
            break;
        }
        default: FATAL(EBADWHENCE);
    }
    return 0;
}

// ============================================================================
// Return the cursor position for the file open on File Descriptor 'fd'
// ============================================================================
i32 fsTell(i32 fd) {
    return bfsTell(fd);
}


// ============================================================================
// Retrieve the current file size in bytes.  This depends on the highest offset
// written to the file, or the highest offset set with the fsSeek function.  On
// success, return the file size.  On failure, abort
// ============================================================================
i32 fsSize(i32 fd) {
    i32 inum = bfsFdToInum(fd);
    return bfsGetSize(inum);
}


// ============================================================================
// Write 'numb' bytes of data from 'buf' into the file currently fsOpen'd on
// filedescriptor 'fd'.  The write starts at the current file offset for the
// destination file.  On success, return 0.  On failure, abort
// ============================================================================
i32 fsWrite(i32 fd, i32 numb, void *buf) {
    if (numb <= 0) FATAL(ENEGNUMB);

    i32 inum = bfsFdToInum(fd);      // Convert file descriptor to inode number
    i32 cursor = bfsTell(fd);        // Get current cursor position
    i32 size = bfsGetSize(inum);     // Get file size

    // Extend file if writing beyond current size
    if (cursor + numb > size) {
        // Calculate last file block needed for this write
        i32 newFbn = (cursor + numb - 1) / BYTESPERBLOCK;
        bfsExtend(inum, newFbn);    // allocate new blocks as needed

        // Zero-fill the gap between old file end and new write location
        if (cursor > size) {
            i32 gapStart = size;
            while (gapStart < cursor) {
                i32 fbn = gapStart / BYTESPERBLOCK;
                i32 dbn = bfsFbnToDbn(inum, fbn);

                // If this block has not been allocated yet, allocate it
                if (dbn == ENODBN) {
                    dbn = bfsAllocBlock(inum, fbn);
                }

                i8 zeroBlock[BYTESPERBLOCK] = {0};
                bioWrite(dbn, zeroBlock);
                gapStart += BYTESPERBLOCK;  // Move to next block
            }
        }
        bfsSetSize(inum, cursor + numb); // Update file size
    }

    i8 *buf8 = (i8 *) buf;
    i32 bytesWritten = 0;

    // Calculate starting file block number (fbn) and offset within that block
    i32 fbn = cursor / BYTESPERBLOCK;
    i32 offset = cursor % BYTESPERBLOCK;

    // Write block by block
    while (bytesWritten < numb) {
        i8 blockBuf[BYTESPERBLOCK] = {0};
        i32 dbn = bfsFbnToDbn(inum, fbn);

        // If block doesn't exist, allocate one
        if (dbn == ENODBN) {
            dbn = bfsAllocBlock(inum, fbn);
        } else {
            // Read existing block if modifying only part of it
            if (offset != 0 || numb - bytesWritten < BYTESPERBLOCK) {
                bioRead(dbn, blockBuf);
            }
        }

        // Calculate bytes to write to this block
        i32 blockBytesToWrite = BYTESPERBLOCK - offset;
        if (bytesWritten + blockBytesToWrite > numb) {
            blockBytesToWrite = numb - bytesWritten;
        }

        // Copy data from input buffer to block buffer
        memcpy(blockBuf + offset, buf8 + bytesWritten, blockBytesToWrite);

        // Write block back to disk
        bioWrite(dbn, blockBuf);

        bytesWritten += blockBytesToWrite;
        offset = 0;                  // Reset offset for next blocks
        fbn++;                       // Move to next block
    }

    // Update cursor position
    bfsSetCursor(inum, cursor + bytesWritten);

    return 0; // Success
}