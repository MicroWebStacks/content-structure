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
    const itemAssetsSchema = requireTableSchema(schema, 'item_assets');
    runInTransaction(db, () => {
        createTables(db, schema);
        syncTableColumns(db, schema);
        resetTables(db, schema);
    });
    const insertDocumentTx = db.transaction((payload) => {
        const {row, items, itemAssets} = payload;
        persistDocuments(db, [row], documentsSchema, {transaction: false});
        persistSimpleRows(db, 'items', itemsSchema.insertColumns, items, {transaction: false});
        persistSimpleRows(db, 'item_assets', itemAssetsSchema.insertColumns, itemAssets, {transaction: false});
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
        insertBlobs(blobsList = []) {
            persistBlobs(db, blobsList, blobsSchema);
        },
        insertAssets(assetsList = []) {
            persistAssets(db, assetsList, assetsSchema);
        },
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
        itemAssets: itemsResult.assets
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
    const itemAssets = [];
    let orderIndex = 0;
    let headingCursor = 0;
    let tableCursor = 0;
    let imageCursor = 0;
    let codeCursor = 0;

    function pushRow({type, text, level, assetRefs = []}) {
        const sanitizedText = typeof text === 'string' ? text : '';
        const itemOrder = orderIndex;
        const itemUid = formatItemUid(doc.sid, itemOrder);
        const assetUidList = attachAssetRefs(assetRefs, itemUid);
        rows.push({
            uid: itemUid,
            version_id: versionId,
            doc_sid: doc.sid,
            type,
            level: Number.isFinite(level) ? level : null,
            order_index: itemOrder,
            body_text: sanitizedText.length ? sanitizedText : null,
            asset_uids: serializeList(assetUidList)
        });
        orderIndex += 1;
    }

    function attachAssetRefs(assetRefs, itemUid) {
        if (!assetRefs?.length) {
            return [];
        }
        const assetUidList = [];
        assetRefs.forEach((ref) => {
            if (!ref?.uid) {
                return;
            }
            assetUidList.push(ref.uid);
            const asset = assetMap.get(ref.uid);
            itemAssets.push({
                ref_id_in_body: ref.refId,
                version_id: versionId,
                doc_sid: doc.sid,
                asset_uid: ref.uid,
                blob_hash: asset?.blob_hash ?? null,
                role: ref.role ?? null
            });
        });
        return assetUidList;
    }

    function createAssetRef(assetUid, role, itemOrder, assetIndex = 0) {
        if (!assetUid || !assetMap.has(assetUid)) {
            return null;
        }
        const refId = buildRefId(doc.sid, versionId, itemOrder, assetIndex);
        return {uid: assetUid, role, refId};
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
            level,
            assetRefs: []
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
                    level,
                    assetRefs: []
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
                        level,
                        assetRefs: []
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
        const itemOrder = orderIndex;
        const assetRefs = [];
        let placeholder = '';
        if (tableEntry.uid) {
            const ref = createAssetRef(tableEntry.uid, 'table_data', itemOrder, assetRefs.length);
            if (ref) {
                assetRefs.push(ref);
                placeholder = formatAssetPlaceholder(ref.refId);
            }
        }
        const descriptionValue = tableEntry.text ?? tableEntry.id ?? 'table';
        const description = String(descriptionValue).trim();
        const text = placeholder ? `${description} ${placeholder}`.trim() : description;
        pushRow({
            type: 'table',
            text,
            level,
            assetRefs
        });
    }

    function handleCode(node) {
        const codeEntry = codeBlocks[codeCursor++] ?? {};
        const line = getNodeLine(node);
        const level = getLevelForLine(line);
        const itemOrder = orderIndex;
        const assetRefs = [];
        let placeholder = '';
        if (codeEntry.uid) {
            const ref = createAssetRef(codeEntry.uid, 'code_block', itemOrder, assetRefs.length);
            if (ref) {
                assetRefs.push(ref);
                placeholder = formatAssetPlaceholder(ref.refId);
            }
        }
        const language = codeEntry.language ?? node.lang ?? 'code';
        const label = `code(${language})`;
        const text = placeholder ? `${label} ${placeholder}`.trim() : label;
        pushRow({
            type: 'code',
            text,
            level,
            assetRefs
        });
    }

    function handleImage(node, levelOverride, lineOverride) {
        const imageEntry = images[imageCursor++] ?? {};
        const line = lineOverride ?? getNodeLine(node);
        const level = typeof levelOverride === 'number' ? levelOverride : getLevelForLine(line);
        const itemOrder = orderIndex;
        const assetRefs = [];
        let placeholder = '';
        if (imageEntry.uid) {
            const ref = createAssetRef(imageEntry.uid, 'inline_image', itemOrder, assetRefs.length);
            if (ref) {
                assetRefs.push(ref);
                placeholder = formatAssetPlaceholder(ref.refId);
            }
        }
        const labelParts = [];
        if (imageEntry.title) {
            labelParts.push(String(imageEntry.title));
        }
        if (imageEntry.alt) {
            labelParts.push(String(imageEntry.alt));
        }
        const label = labelParts.join(' ').trim() || 'image';
        const text = placeholder ? `${label} ${placeholder}`.trim() : label;
        pushRow({
            type: 'image',
            text,
            level,
            assetRefs
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
    return {rows, assets: itemAssets};
}

function formatItemUid(docSid, orderIndex) {
    const orderStr = String(orderIndex + 1).padStart(4, '0');
    return `${docSid}-I${orderStr}`;
}

function buildRefId(docSid, versionId, orderIndex, assetIndex) {
    const orderStr = String(orderIndex + 1).padStart(4, '0');
    const assetStr = String(assetIndex + 1).padStart(2, '0');
    return `${versionId}-${docSid}-I${orderStr}-A${assetStr}`;
}

function formatAssetPlaceholder(refId) {
    return `{{asset:${refId}}}`;
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
        first_seen: blob.first_seen ?? null,
        last_seen: blob.last_seen ?? null,
        year: blob.year ?? null,
        month: blob.month ?? null,
        prefix: blob.prefix ?? null
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
