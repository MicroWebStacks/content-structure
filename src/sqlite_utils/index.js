import Database from 'better-sqlite3';

const GLOBAL_CACHE_KEY = Symbol.for('content-structure.sqlite-cache');

function getGlobalCache() {
    if (!globalThis[GLOBAL_CACHE_KEY]) {
        globalThis[GLOBAL_CACHE_KEY] = new Map();
    }
    return globalThis[GLOBAL_CACHE_KEY];
}

function openDatabase(filePath, options = {}) {
    const cache = getGlobalCache();
    if (!cache.has(filePath)) {
        const db = new Database(filePath, options);
        db.pragma('journal_mode = WAL');
        db.pragma('synchronous = NORMAL');
        db.pragma('foreign_keys = ON');
        db.__contentStructureColumns = new Map();
        cache.set(filePath, db);
    }
    return cache.get(filePath);
}

function getColumnSet(db, tableName) {
    if (!db.__contentStructureColumns.has(tableName)) {
        const rows = db.prepare(`PRAGMA table_info("${tableName}")`).all();
        const columns = new Set(rows.map((row) => row.name));
        db.__contentStructureColumns.set(tableName, columns);
    }
    return db.__contentStructureColumns.get(tableName);
}

function ensureColumn(db, tableName, columnName, type = 'TEXT') {
    const columns = getColumnSet(db, tableName);
    if (columns.has(columnName)) {
        return;
    }
    db.prepare(`ALTER TABLE "${tableName}" ADD COLUMN "${columnName}" ${type}`).run();
    columns.add(columnName);
}

function ensureTable(db, tableName, columnsSql) {
    db.prepare(`CREATE TABLE IF NOT EXISTS "${tableName}" (${columnsSql})`).run();
    db.__contentStructureColumns.set(
        tableName,
        new Set(db.prepare(`PRAGMA table_info("${tableName}")`).all().map((row) => row.name))
    );
}

function clearTable(db, tableName) {
    db.prepare(`DELETE FROM "${tableName}"`).run();
}

function runInTransaction(db, callback) {
    const tx = db.transaction(callback);
    return tx();
}

function insertRows(db, tableName, columns, rows) {
    if (!rows.length) {
        return;
    }
    const columnSql = columns.map((col) => `"${col}"`).join(', ');
    const placeholderSql = columns.map((col) => `@${col}`).join(', ');
    const statement = db.prepare(`INSERT INTO "${tableName}" (${columnSql}) VALUES (${placeholderSql})`);
    const execute = db.transaction((batch) => {
        for (const row of batch) {
            const params = {};
            for (const column of columns) {
                params[column] = Object.hasOwn(row, column) ? row[column] : null;
            }
            statement.run(params);
        }
    });
    execute(rows);
}

export {
    openDatabase,
    ensureColumn,
    ensureTable,
    clearTable,
    runInTransaction,
    insertRows
};
