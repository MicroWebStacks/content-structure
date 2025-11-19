import { createHash } from 'crypto';
import { readFile, writeFile } from 'fs/promises';
import { join } from 'path';
import { check_dir_create, exists_abs } from './utils.js';
import { get_config } from './collect.js';

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

class BlobManager {
    constructor(timestamp) {
        this.timestamp = timestamp ?? new Date().toISOString();
        this.hashIndex = new Map(); // hash -> {relativePath, size}
    }

    async ensureFromBuffer(buffer) {
        if (!buffer) {
            return null;
        }
        const hash = createHash('sha512').update(buffer).digest('hex');
        const existing = this.hashIndex.get(hash);
        if (!existing) {
            const {year, month} = getDateParts(this.timestamp);
            const prefix = hash.slice(0, 2);
            const relDir = ['blobs', String(year), formatMonth(month), prefix].join('/');
            await check_dir_create(relDir);
            const relativePath = `${relDir}/${hash}`;
            await this.writeBlob(relativePath, buffer);
            const entry = {
                relativePath,
                size: buffer.length
            };
            this.hashIndex.set(hash, entry);
            return {hash, size: entry.size, path: entry.relativePath};
        }
        return {hash, size: existing.size, path: existing.relativePath};
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
}

function createBlobManager(timestamp) {
    return new BlobManager(timestamp);
}

export {
    createBlobManager
};
