import { join } from 'path';
import { check_dir_create, load_yaml_code } from './utils.js';
import { get_config } from './collect.js';
import { warn } from './libs/log.js';
import { openDatabase, ensureTable, clearTable, insertRows, runInTransaction, ensureColumn } from './sqlite_utils/index.js';
import { computeVersionId } from './version_id.js';
import { node_text } from './md_utils.js';

const DB_FILENAME = 'structure.db';
const CATALOG_PATH = 'catalog.yaml';
const STRUCTURE_DATASET_NAME = 'structure';
const LIST_COLUMN_TYPES = new Set(['string_list', 'object_list']);

let structureSchemaPromise;

async function getStructureSchema() {
    if (!structureSchemaPromise) {
        structureSchemaPromise = loadStructureSchema();
    }
    return structureSchemaPromise;
}

async function loadStructureSchema() {
    const catalog = await load_yaml_code(CATALOG_PATH);
    const dataset = catalog?.datasets?.find((entry) => entry.name === STRUCTURE_DATASET_NAME);
    if (!dataset) {
        throw new Error(`catalog.yaml missing dataset '${STRUCTURE_DATASET_NAME}'`);
    }
    const tables = new Map();
    for (const table of dataset.tables ?? []) {
        const normalized = normalizeTableSchema(table);
        tables.set(normalized.name, normalized);
    }
    return { tables };
}

function normalizeTableSchema(table) {
    const columns = (table.columns ?? []).map((column) => normalizeColumnSchema(column));
    return {
        name: table.name,
        columns,
        columnLookup: new Map(columns.map((column) => [column.name, column])),
        insertColumns: columns.filter((column) => !column.autoIncrement).map((column) => column.name),
        createSql: buildCreateSql(columns)
    };
}

function normalizeColumnSchema(column) {
    const type = column.type ?? 'string';
    const sqliteType = mapColumnTypeToSqlite(type);
    const isList = LIST_COLUMN_TYPES.has(type);
    const autoIncrement = column.autoincrement ?? (column.primary && type === 'int' && column.name === 'id');
    return {
        ...column,
        type,
        sqliteType,
        isList,
        autoIncrement
    };
}

function buildCreateSql(columns) {
    return columns.map((column) => buildColumnSql(column)).join(',\n        ');
}

function buildColumnSql(column) {
    let sql = `"${column.name}" ${column.sqliteType}`;
    if (column.primary) {
        if (column.sqliteType === 'INTEGER' && column.autoIncrement) {
            sql += ' PRIMARY KEY AUTOINCREMENT';
        } else {
            sql += ' PRIMARY KEY';
        }
    }
    return sql;
}

function mapColumnTypeToSqlite(type) {
    switch (type) {
        case 'int':
        case 'boolean':
            return 'INTEGER';
        default:
            return 'TEXT';
    }
}

function requireTableSchema(schema, tableName) {
    const table = schema.tables.get(tableName);
    if (!table) {
        throw new Error(`catalog.yaml missing table '${tableName}' in dataset '${STRUCTURE_DATASET_NAME}'`);
    }
    return table;
}

async function createStructureDbWriter(options = {}) {
    const config = get_config();
    await check_dir_create('');
    const dbPath = join(config.outdir, DB_FILENAME);
    const schema = await getStructureSchema();
    const versionId = options?.versionId ?? computeVersionId();
    let db;
    try {
        db = openDatabase(dbPath);
    } catch (error) {
        warn(`(!) skipping structure.db generation: ${error.message}`);
        return null;
    }
    const documentsSchema = requireTableSchema(schema, 'documents');
    const assetsSchema = requireTableSchema(schema, 'assets');
    const blobsSchema = requireTableSchema(schema, 'blobs');
    const itemsSchema = requireTableSchema(schema, 'items');
    const assetVersionSchema = requireTableSchema(schema, 'asset_version');
    runInTransaction(db, () => {
        createTables(db, schema);
        syncTableColumns(db, schema);
        resetTables(db, schema);
    });
    const insertDocumentTx = db.transaction((payload) => {
        const {row, items, assetVersions} = payload;
        persistDocuments(db, [row], documentsSchema, {transaction: false});
        persistSimpleRows(db, 'items', itemsSchema.insertColumns, items, {transaction: false});
        persistSimpleRows(db, 'asset_version', assetVersionSchema.insertColumns, assetVersions, {transaction: false});
    });
    return {
        insertDocument(entry, content, tree, assets) {
            if (!entry) {
                return;
            }
            const payload = buildDocumentRow(entry, content, documentsSchema, {
                versionId,
                tree,
                assets
            });
            insertDocumentTx(payload);
        },
        insertAssets(assetsList = []) {
            persistAssets(db, assetsList, assetsSchema);
        },
        insertBlobs(blobsList = []) {
            persistBlobs(db, blobsList, blobsSchema);
        }
    };
}

async function writeStructureDb({
    documents = [],
    assets = [],
    blobs = [],
    documentContents,
    documentTrees,
    documentAssetsBySid,
    versionId
}) {
    const writer = await createStructureDbWriter({versionId});
    if (!writer) {
        return;
    }
    const contentMap = normalizeContentMap(documentContents);
    const treeMap = normalizeContentMap(documentTrees);
    const assetsMap = normalizeContentMap(documentAssetsBySid);
    for (const doc of documents) {
        const content = contentMap.get(doc.sid);
        const tree = treeMap.get(doc.sid);
        const docAssets = assetsMap.get(doc.sid);
        writer.insertDocument(doc, content, tree, docAssets);
    }
    if (assets.length) {
        writer.insertAssets(assets);
    }
    if (blobs.length) {
        writer.insertBlobs(blobs);
    }
}

function createTables(db, schema) {
    for (const table of schema.tables.values()) {
        ensureTable(db, table.name, table.createSql);
    }
}

function syncTableColumns(db, schema) {
    for (const table of schema.tables.values()) {
        for (const column of table.columns) {
            ensureColumn(db, table.name, column.name, column.sqliteType);
        }
    }
}

function resetTables(db, schema) {
    for (const table of schema.tables.values()) {
        clearTable(db, table.name);
    }
}

function normalizeContentMap(documentContents) {
    if (!documentContents) {
        return new Map();
    }
    if (documentContents instanceof Map) {
        return documentContents;
    }
    return new Map(Object.entries(documentContents));
}

function buildDocumentRow(doc, content, documentSchema, options = {}) {
    const itemsResult = buildItemRows(doc, content, options);
    const row = {};
    for (const column of documentSchema.columns) {
        row[column.name] = getDocumentColumnValue(column, doc, content);
    }
    return {
        row,
        items: itemsResult.rows,
        assetVersions: itemsResult.assetVersions
    };
}

function getDocumentColumnValue(column, doc) {
    return formatColumnValue(column, doc[column.name]);
}

function formatColumnValue(column, value) {
    if (column.isList) {
        return serializeList(normalizeListValue(value));
    }
    if (value === undefined) {
        return null;
    }
    if (column.type === 'boolean') {
        return normalizeScalar(value);
    }
    return value ?? null;
}

function normalizeListValue(value) {
    if (Array.isArray(value)) {
        return value;
    }
    if (value === null || value === undefined) {
        return [];
    }
    return [value];
}

function buildItemRows(doc, content, options = {}) {
    const tree = options?.tree;
    if (!tree?.children || !Array.isArray(tree.children) || tree.children.length === 0) {
        return {rows: [], assets: []};
    }
    const versionId = options?.versionId ?? computeVersionId();
    const headings = Array.isArray(content?.headings) ? content.headings : [];
    const tables = Array.isArray(content?.tables) ? content.tables : [];
    const images = Array.isArray(content?.images) ? content.images : [];
    const codeBlocks = Array.isArray(content?.code) ? content.code : [];
    const assets = Array.isArray(options?.assets) ? options.assets : [];
    const assetMap = new Map();
    for (const asset of assets) {
        if (asset?.uid) {
            assetMap.set(asset.uid, asset);
        }
    }
    const rows = [];
    const assetVersions = [];
    const recordedAssetKeys = new Set();
    let orderIndex = 0;
    let headingCursor = 0;
    let tableCursor = 0;
    let imageCursor = 0;
    let codeCursor = 0;

    function pushRow({type, text, level}) {
        const sanitizedText = typeof text === 'string' ? text : '';
        const itemOrder = orderIndex;
        const itemUid = formatItemUid(doc.sid, itemOrder);
        rows.push({
            uid: itemUid,
            version_id: versionId,
            doc_sid: doc.sid,
            type,
            level: Number.isFinite(level) ? level : null,
            order_index: itemOrder,
            body_text: sanitizedText.length ? sanitizedText : null
        });
        orderIndex += 1;
    }

    function recordAssetVersion(assetUid, role) {
        if (!assetUid) {
            return null;
        }
        const asset = assetMap.get(assetUid);
        if (!asset) {
            return null;
        }
        const key = assetUid;
        if (!recordedAssetKeys.has(key)) {
            recordedAssetKeys.add(key);
            assetVersions.push({
                asset_uid: asset.uid,
                version_id: versionId,
                doc_sid: doc.sid,
                blob_hash: asset.blob_hash ?? null,
                role: role ?? null
            });
        }
        return asset;
    }

    function createAssetLink(assetUid, role, labelText) {
        const asset = recordAssetVersion(assetUid, role);
        if (!asset) {
            return null;
        }
        const normalizedLabel = formatAssetLabel(labelText, asset.uid);
        const schemeType = formatAssetType(asset.type);
        return `![${normalizedLabel}](asset://${schemeType}/${asset.uid})`;
    }

    function formatAssetLabel(labelText, fallback) {
        const raw = typeof labelText === 'string' ? labelText.trim() : '';
        const base = raw || fallback || 'asset';
        return base.replace(/[\[\]]/g, '');
    }

    function formatAssetType(type) {
        if (!type) {
            return 'asset';
        }
        return String(type).trim().replace(/[^a-zA-Z0-9_.-]+/g, '-').toLowerCase();
    }

    function processNode(node) {
        if (!node) {
            return;
        }
        switch (node.type) {
            case 'heading':
                handleHeading(node);
                return;
            case 'paragraph':
                handleParagraph(node);
                return;
            case 'table':
                handleTable(node);
                return;
            case 'code':
                handleCode(node);
                return;
            case 'image':
                handleImage(node);
                return;
            default:
                if (Array.isArray(node.children)) {
                    node.children.forEach(processNode);
                }
        }
    }

    function handleHeading(node) {
        const headingEntry = headings[headingCursor++] ?? null;
        const text = headingEntry?.label ?? node_text(node) ?? '';
        const level = headingEntry?.depth ?? node.depth ?? 0;
        pushRow({
            type: 'heading',
            text,
            level
        });
    }

    function handleParagraph(node) {
        const line = getNodeLine(node);
        const level = getLevelForLine(line);
        const segments = splitParagraphSegments(node);
        if (!segments.length) {
            const fallbackText = node_text(node) ?? '';
            if (fallbackText && fallbackText.trim()) {
                pushRow({
                    type: 'paragraph',
                    text: fallbackText,
                    level
                });
            }
            return;
        }
        segments.forEach((segment) => {
            if (segment.type === 'text') {
                if (segment.value) {
                    pushRow({
                        type: 'paragraph',
                        text: segment.value,
                        level
                    });
                }
                return;
            }
            if (segment.type === 'image') {
                handleImage(segment.node, level, line);
            }
        });
    }

    function handleTable(node) {
        const tableEntry = tables[tableCursor++] ?? {};
        const line = getNodeLine(node);
        const level = getLevelForLine(line);
        const descriptionValue = tableEntry.text ?? tableEntry.id ?? 'table';
        const description = String(descriptionValue).trim();
        const assetLink = createAssetLink(tableEntry.uid, 'table_data', description);
        const text = assetLink ?? (description || 'table');
        pushRow({
            type: 'table',
            text,
            level
        });
    }

    function handleCode(node) {
        const codeEntry = codeBlocks[codeCursor++] ?? {};
        const line = getNodeLine(node);
        const level = getLevelForLine(line);
        const language = codeEntry.language ?? node.lang ?? 'code';
        const label = `code(${language})`;
        const assetLink = createAssetLink(codeEntry.uid, 'code_block', label);
        const text = assetLink ?? label;
        pushRow({
            type: 'code',
            text,
            level
        });
    }

    function handleImage(node, levelOverride, lineOverride) {
        const imageEntry = images[imageCursor++] ?? {};
        const line = lineOverride ?? getNodeLine(node);
        const level = typeof levelOverride === 'number' ? levelOverride : getLevelForLine(line);
        const labelParts = [];
        if (imageEntry.title) {
            labelParts.push(String(imageEntry.title));
        }
        if (imageEntry.alt) {
            labelParts.push(String(imageEntry.alt));
        }
        const label = labelParts.join(' ').trim() || 'image';
        const assetLink = createAssetLink(imageEntry.uid, 'inline_image', label);
        const text = assetLink ?? label;
        pushRow({
            type: 'image',
            text,
            level
        });
    }

    function splitParagraphSegments(node) {
        if (!Array.isArray(node.children) || node.children.length === 0) {
            return [];
        }
        const segments = [];
        let buffer = [];
        function flushBuffer() {
            if (!buffer.length) {
                return;
            }
            const synthetic = {type: 'paragraph', children: buffer};
            const text = node_text(synthetic);
            buffer = [];
            if (text && text.trim()) {
                segments.push({type: 'text', value: text});
            }
        }
        for (const child of node.children) {
            if (child.type === 'image') {
                flushBuffer();
                segments.push({type: 'image', node: child});
            } else {
                buffer.push(child);
            }
        }
        flushBuffer();
        return segments;
    }

    function getLevelForLine(line) {
        const context = findHeadingContext(line, headings);
        if (context?.depth != null) {
            return context.depth;
        }
        return 0;
    }

    tree.children.forEach(processNode);
    return {rows, assetVersions};
}

function formatItemUid(docSid, orderIndex) {
    const orderStr = String(orderIndex + 1).padStart(4, '0');
    return `${docSid}-I${orderStr}`;
}

function getNodeLine(node) {
    return node?.position?.start?.line ?? null;
}

function findHeadingContext(line, headings) {
    if (!Number.isFinite(line) || !Array.isArray(headings) || !headings.length) {
        return null;
    }
    for (let index = headings.length - 1; index >= 0; index -= 1) {
        const entry = headings[index];
        if (entry?.line < line) {
            return entry;
        }
    }
    return null;
}

function persistDocuments(db, rows, documentSchema, options) {
    if (!rows.length) {
        return;
    }
    insertRows(db, 'documents', documentSchema.insertColumns, rows, options);
}

function persistSimpleRows(db, tableName, columns, rows, options) {
    if (!rows.length) {
        return;
    }
    insertRows(db, tableName, columns, rows, options);
}

function persistAssets(db, assets, assetsSchema, options) {
    if (!assets.length) {
        return;
    }
    const rows = assets.map((asset) => ({
        uid: asset.uid,
        type: asset.type ?? null,
        blob_hash: asset.blob_hash ?? null,
        parent_doc_uid: asset.parent_doc_uid ?? null,
        path: asset.path ?? null,
        ext: asset.ext ?? null,
        first_seen: asset.first_seen ?? null,
        last_seen: asset.last_seen ?? null
    }));
    insertRows(db, 'assets', assetsSchema.insertColumns, rows, options);
}

function persistBlobs(db, blobs, blobsSchema, options) {
    if (!blobs.length) {
        return;
    }
    const rows = blobs.map((blob) => ({
        hash: blob.hash,
        size: blob.size ?? null,
        path: blob.path ?? null,
        first_seen: blob.first_seen ?? null,
        last_seen: blob.last_seen ?? null
    }));
    insertRows(db, 'blobs', blobsSchema.insertColumns, rows, options);
}

function serializeList(list) {
    const value = Array.isArray(list) ? list : [];
    return JSON.stringify(value);
}

/**
 * Normalize scalar values for storage, accepting booleans, numbers, strings, null, or undefined.
 * @param {boolean | number | string | null | undefined} value
 * @returns {number | string | null}
 */
function normalizeScalar(value) {
    if (value === null || value === undefined) {
        return null;
    }
    if (typeof value === 'boolean') {
        return value ? 1 : 0;
    }
    if (typeof value === 'string') {
        const lower = value.trim().toLowerCase();
        if (lower === 'true' || lower === '1') return 1;
        if (lower === 'false' || lower === '0') return 0;
    }
    return value;
}

export {
    writeStructureDb,
    createStructureDbWriter,
    getStructureSchema
};
