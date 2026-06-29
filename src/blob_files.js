// Shared helpers for materializing content-addressed blob files on disk.
//
// Blob assets are written as <blobsDir>/<hash>.<ext> (decompressed) so they can
// be served as immutable, browser-cacheable static files. The file-name formula
// is shared with the consumer's URL resolver — keep both in sync.
//
// Pure / no native deps (fs + zlib only), so the JSON ("lite") writer can use it
// without pulling in anything the SQLite writer needs.
import { join } from 'path';
import { readFile, writeFile } from 'fs/promises';
import { gunzipSync } from 'zlib';

function normalizeExt(ext) {
    const value = String(ext ?? '').trim().toLowerCase();
    if (!value) {
        return '';
    }
    return value.startsWith('.') ? value.slice(1) : value;
}

// Content-addressed file name: <hash>.<ext>, or the bare hash when ext is empty.
function blobFileName(hash, ext) {
    const normalized = normalizeExt(ext);
    return normalized ? `${hash}.${normalized}` : String(hash);
}

// Resolve the decompressed bytes for a blob row, either from an inline payload
// or from the external blob file under <outdir>/blobs/<path>/<hash>.
async function resolveBlobBytes(blob, outdir) {
    let buffer = null;
    if (blob.payload) {
        buffer = Buffer.isBuffer(blob.payload) ? blob.payload : Buffer.from(blob.payload);
    } else if (blob.path && blob.hash) {
        try {
            buffer = await readFile(join(outdir, 'blobs', blob.path, blob.hash));
        } catch {
            buffer = null;
        }
    }
    if (!buffer) {
        return null;
    }
    if (blob.compression) {
        try {
            buffer = gunzipSync(buffer);
        } catch {
            /* leave as-is if it was not actually gzip-compressed */
        }
    }
    return buffer;
}

// Materialize <hash>.<ext> files into blobsDir for a set of (blob, ext) refs.
// `refs` is an iterable of {blob, ext}; `blob` is a row carrying hash/payload/
// path/compression. Dedups by file name and by resolved bytes. Returns counts.
async function writeBlobFiles(refs, blobsDir, outdir) {
    const written = new Set();
    const bytesCache = new Map();
    let count = 0;
    let missing = 0;
    for (const {blob, ext} of refs) {
        if (!blob?.hash) {
            continue;
        }
        const fileName = blobFileName(blob.hash, ext);
        if (written.has(fileName)) {
            continue;
        }
        let bytes = bytesCache.get(blob.hash);
        if (bytes === undefined) {
            bytes = await resolveBlobBytes(blob, outdir);
            bytesCache.set(blob.hash, bytes);
        }
        if (!bytes) {
            missing += 1;
            continue;
        }
        await writeFile(join(blobsDir, fileName), bytes);
        written.add(fileName);
        count += 1;
    }
    return {count, missing};
}

export {
    normalizeExt,
    blobFileName,
    resolveBlobBytes,
    writeBlobFiles
};
