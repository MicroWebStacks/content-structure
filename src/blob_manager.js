import { createHash } from 'crypto';
import { readFile, writeFile } from 'fs/promises';
import { join } from 'path';
import { gzip } from 'zlib';
import { promisify } from 'util';
import { check_dir_create, exists_abs } from './utils.js';
import { get_config } from './collect.js';

const gzipAsync = promisify(gzip);
const DEFAULT_EXTERNAL_THRESHOLD_BYTES = 1024 * 1024; // 1MB
const DEFAULT_INLINE_COMPRESSION_MIN_BYTES = 4 * 1024; // 4KB

function formatMonth(month) {
    return String(month).padStart(2, '0');
}

function getDateParts(timestamp) {
    const date = new Date(timestamp);
    return {
        year: date.getUTCFullYear(),
        month: date.getUTCMonth() + 1
    };
}

function deriveStorageDir(relativePath) {
    if (!relativePath) {
        return null;
    }
    const segments = relativePath.split('/').filter(Boolean);
    if (!segments.length) {
        return null;
    }
    const startIndex = segments[0] === 'blobs' ? 1 : 0;
    if (segments.length - startIndex >= 3) {
        return segments.slice(startIndex, startIndex + 3).join('/');
    }
    return segments.slice(startIndex).join('/');
}

class BlobManager {
    constructor(timestamp) {
        this.timestamp = timestamp ?? new Date().toISOString();
        const config = get_config();
        this.externalThreshold = normalizeThreshold(
            config?.blob_external_threshold_bytes,
            DEFAULT_EXTERNAL_THRESHOLD_BYTES
        );
        this.inlineCompressionMin = normalizeThreshold(
            config?.blob_inline_compression_min_bytes,
            DEFAULT_INLINE_COMPRESSION_MIN_BYTES
        );
        this.hashIndex = new Map(); // hash -> {relativePath, storageDir, size, payload, compression}
    }

    async ensureFromBuffer(buffer) {
        if (!buffer) {
            return null;
        }
        const hash = createHash('sha512').update(buffer).digest('hex');
        const existing = this.hashIndex.get(hash);
        if (!existing) {
            const entry = await this.persistBuffer(buffer, hash);
            this.hashIndex.set(hash, entry);
            return buildResult(hash, entry);
        }
        return buildResult(hash, existing);
    }

    async ensureFromFile(absPath) {
        if (!absPath) {
            return null;
        }
        const buffer = await readFile(absPath);
        return this.ensureFromBuffer(buffer);
    }

    async writeBlob(relativePath, buffer) {
        const config = get_config();
        const segments = relativePath.split('/');
        const absPath = join(config.outdir, ...segments);
        if (await exists_abs(absPath)) {
            return;
        }
        await writeFile(absPath, buffer);
    }

    async persistBuffer(buffer, hash) {
        const size = buffer.length;
        if (size > this.externalThreshold) {
            const {year, month} = getDateParts(this.timestamp);
            const prefix = hash.slice(0, 2);
            const storageDir = [String(year), formatMonth(month), prefix].join('/');
            const relDir = ['blobs', storageDir].join('/');
            await check_dir_create(relDir);
            const relativePath = `${relDir}/${hash}`;
            await this.writeBlob(relativePath, buffer);
            return {
                relativePath,
                storageDir,
                size,
                payload: null,
                compression: null
            };
        }
        const inlinePayload = await this.prepareInlinePayload(buffer);
        return {
            relativePath: null,
            storageDir: null,
            size,
            payload: inlinePayload.payload,
            compression: inlinePayload.compression
        };
    }

    async prepareInlinePayload(buffer) {
        const shouldCompress = buffer.length >= this.inlineCompressionMin;
        if (!shouldCompress) {
            return {
                payload: Buffer.from(buffer),
                compression: false
            };
        }
        const compressed = await gzipAsync(buffer);
        return {
            payload: compressed,
            compression: true
        };
    }
}

function normalizeThreshold(value, fallback) {
    if (value === null || value === undefined) {
        return fallback;
    }
    const numeric = Number(value);
    if (!Number.isFinite(numeric) || numeric <= 0) {
        return fallback;
    }
    return numeric;
}

function buildResult(hash, entry) {
    const storageDir = entry.storageDir ?? deriveStorageDir(entry.relativePath);
    return {
        hash,
        size: entry.size,
        path: storageDir ?? null,
        payload: entry.payload ?? null,
        compression: entry.compression ?? null
    };
}

function createBlobManager(timestamp) {
    return new BlobManager(timestamp);
}

export {
    createBlobManager
};
