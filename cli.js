#!/usr/bin/env node
import { join, resolve } from 'path';
import { stat, access } from 'fs/promises';
import { constants as fsConstants } from 'fs';
import Database from 'better-sqlite3';
import { collect } from './index.js';

async function main() {
    const [, , contentArg] = process.argv;
    if (!contentArg || contentArg === '--help' || contentArg === '-h') {
        printUsage();
        process.exit(contentArg ? 0 : 1);
    }

    const rootdir = process.cwd();
    const contentdir = resolve(rootdir, contentArg);
    const outdir = join(rootdir, '.structure');
    const dbName = 'structure.db';

    await assertDirectory(contentdir);

    const dbPath = join(outdir, dbName);
    console.log(`content-structure: reading from ${contentdir}`);
    console.log(`content-structure: writing to ${dbPath}`);
    await collect({
        rootdir,
        contentdir,
        outdir,
        db_name: dbName
    });
    await printDbSummary(join(outdir, dbName));
    console.log('content-structure: done');
}

async function assertDirectory(pathValue) {
    try {
        const stats = await stat(pathValue);
        if (!stats.isDirectory()) {
            throw new Error(`'${pathValue}' is not a directory`);
        }
    } catch (error) {
        console.error(`content-structure: ${error.message}`);
        process.exit(1);
    }
}

function printUsage() {
    // Keep short output for npx usage
    console.log('Usage: content-structure <content_dir>');
    console.log('Example: npx content-structure ./example/content');
}

async function pathExists(pathValue) {
    try {
        await access(pathValue, fsConstants.F_OK);
        return true;
    } catch {
        return false;
    }
}

async function printDbSummary(dbPath) {
    const exists = await pathExists(dbPath);
    if (!exists) {
        console.log(`content-structure: no database found at ${dbPath}`);
        return;
    }
    try {
        const db = new Database(dbPath, {readonly: true});
        const tables = db.prepare(`
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        `).all();
        if (!tables.length) {
            console.log('content-structure: database has no tables');
            db.close();
            return;
        }
        console.log('content-structure: tables and row counts:');
        for (const {name} of tables) {
            const {count} = db.prepare(`SELECT COUNT(*) AS count FROM "${name}"`).get();
            console.log(`  - ${name}: ${count}`);
        }
        db.close();
    } catch (error) {
        console.error(`content-structure: failed reading ${dbPath}: ${error.message}`);
    }
}

main().catch((error) => {
    console.error(`content-structure: ${error.message}`);
    process.exit(1);
});
