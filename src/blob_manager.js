import { createHash } from 'crypto';
import { readFile, writeFile } from 'fs/promises';
import { extname, join } from 'path';
import { gzip } from 'zlib';
import { promisify } from 'util';
import { check_dir_create, exists_abs } from './utils.js';
import { get_config } from './collect.js';

const gzipAsync = promisify(gzip);
const DEFAULT_EXTERNAL_THRESHOLD_BYTES = 512 * 1024;
const DEFAULT_INLINE_COMPRESSION_MIN_BYTES = 32 * 1024;
const DEFAULT_COMPRESSIBLE_EXTENSIONS = new Set(['txt', 'md', 'json', 'csv', 'tsv', 'yaml', 'yml']);

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
        const legacyExternalThreshold = config?.blob_external_threshold_bytes;
        const legacyInlineCompression = config?.blob_inline_compression_min_bytes;
        this.externalThreshold = normalizeThreshold(
            Number.isFinite(Number(config?.external_storage_kb))
                ? Number(config.external_storage_kb) * 1024
                : legacyExternalThreshold,
            DEFAULT_EXTERNAL_THRESHOLD_BYTES
        );
        this.inlineCompressionMin = normalizeThreshold(
            Number.isFinite(Number(config?.inline_compression_kb))
                ? Number(config.inline_compression_kb) * 1024
                : legacyInlineCompression,
            DEFAULT_INLINE_COMPRESSION_MIN_BYTES
        );
        this.compressibleExtensions = buildCompressibleExtensionSet(config?.file_compress_ext);
        this.hashIndex = new Map(); // hash -> {relativePath, storageDir, size, payload, compression}
    }

    async ensureFromBuffer(buffer, options = {}) {
        if (!buffer) {
            return null;
        }
        const hash = createHash('sha512').update(buffer).digest('hex');
        const existing = this.hashIndex.get(hash);
        if (!existing) {
            const entry = await this.persistBuffer(buffer, hash, options);
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
        const compressionHint = inferCompressionHintFromPath(absPath, this.compressibleExtensions);
        return this.ensureFromBuffer(buffer, {compressionHint});
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

    async persistBuffer(buffer, hash, options = {}) {
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
        const inlinePayload = await this.prepareInlinePayload(buffer, options);
        return {
            relativePath: null,
            storageDir: null,
            size,
            payload: inlinePayload.payload,
            compression: inlinePayload.compression
        };
    }

    async prepareInlinePayload(buffer, options = {}) {
        const shouldCompress = this.shouldCompressInlinePayload(buffer.length, options?.compressionHint);
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

    shouldCompressInlinePayload(byteLength, compressionHint) {
        if (byteLength < this.inlineCompressionMin) {
            return false;
        }
        if (!compressionHint) {
            return true;
        }
        return compressionHint.shouldCompress === true;
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

function buildCompressibleExtensionSet(value) {
    if (!value) {
        return new Set(DEFAULT_COMPRESSIBLE_EXTENSIONS);
    }
    const list = Array.isArray(value) ? value : String(value).split(',');
    const normalized = list
        .map((entry) => String(entry ?? '').trim().replace(/^\./, '').toLowerCase())
        .filter(Boolean);
    if (!normalized.length) {
        return new Set(DEFAULT_COMPRESSIBLE_EXTENSIONS);
    }
    return new Set(normalized);
}

function inferCompressionHintFromPath(absPath, compressibleExtensions) {
    if (!absPath) {
        return null;
    }
    const extension = extname(absPath).replace(/^\./, '').toLowerCase();
    if (!extension) {
        return null;
    }
    return {
        shouldCompress: compressibleExtensions.has(extension)
    };
}

function createBlobManager(timestamp) {
    return new BlobManager(timestamp);
}

export {
    createBlobManager
};
