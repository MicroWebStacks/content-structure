import { join } from 'path';
import { check_dir_create, load_yaml_code } from './utils.js';
import { get_config, shortMD5 } from './collect.js';
import { warn } from './libs/log.js';
import { openDatabase, ensureTable, insertRows, runInTransaction, ensureColumn } from './sqlite_utils/index.js';
import { computeVersionId } from './version_id.js';
import { node_text, title_slug } from './md_utils.js';

const DB_FILENAME = 'structure.db';
const CATALOG_PATH = 'catalog.yaml';
const STRUCTURE_DATASET_NAME = 'structure';
const LIST_COLUMN_TYPES = new Set(['string_list', 'object_list']);
const INLINE_COMPLEX_NODE_TYPES = new Set(['strong', 'emphasis', 'delete', 'inlineCode', 'code', 'html', 'link', 'image']);
const TABLE_COMPLEX_NODE_TYPES = new Set(['image', 'link', 'strong', 'emphasis', 'delete', 'inlineCode', 'code', 'html']);
const VERSION_TYPE_VALUES = new Set(['daily', 'weekly', 'monthly', 'early', 'baseline']);

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
        case 'blob':
            return 'BLOB';
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
    const runDate = options?.runDate ?? new Date();
    let db;
    try {
        db = openDatabase(dbPath);
    } catch (error) {
        warn(`(!) skipping structure.db generation: ${error.message}`);
        return null;
    }
    const documentsSchema = requireTableSchema(schema, 'documents');
    const assetInfoSchema = requireTableSchema(schema, 'asset_info');
    const blobStoreSchema = requireTableSchema(schema, 'blob_store');
    const itemsSchema = requireTableSchema(schema, 'items');
    const assetsSchema = requireTableSchema(schema, 'assets');
    const imagesSchema = requireTableSchema(schema, 'images');
    const versionsSchema = requireTableSchema(schema, 'versions');
    runInTransaction(db, () => {
        createTables(db, schema);
        reconcileTables(db, schema);
        syncTableColumns(db, schema);
    });
    const existingState = loadExistingState(db);
    const versionRow = buildVersionRow(versionId, runDate, config, existingState);
    if (versionRow) {
        persistVersions(db, [versionRow], versionsSchema, {transaction: false});
        existingState.versionIds.add(versionId);
    }
    const insertDocumentTx = db.transaction((payload) => {
        const {row, items, assetVersions} = payload;
        persistDocuments(db, [row], documentsSchema, {transaction: false});
        persistSimpleRows(db, 'items', itemsSchema.insertColumns, items, {transaction: false});
        persistSimpleRows(db, 'assets', assetsSchema.insertColumns, assetVersions, {transaction: false});
    });
    return {
        existingState,
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
            persistAssetInfo(db, assetsList, assetInfoSchema, existingState);
        },
        insertBlobs(blobsList = []) {
            persistBlobStore(db, blobsList, blobStoreSchema, existingState);
        },
        insertImages(imagesList = []) {
            persistImages(db, imagesList, imagesSchema, existingState);
        }
    };
}

async function writeStructureDb({
    documents = [],
    assets = [],
    blobs = [],
    images = [],
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
    if (images.length) {
        writer.insertImages(images);
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

function reconcileTables(db, schema) {
    for (const table of schema.tables.values()) {
        const tableInfo = getTableInfo(db, table.name);
        if (!tableInfo.length) {
            ensureTable(db, table.name, table.createSql);
            continue;
        }
        if (tableNeedsRebuild(table, tableInfo)) {
            rebuildTable(db, table, tableInfo);
        }
    }
}

function getTableInfo(db, tableName) {
    try {
        return db.prepare(`PRAGMA table_info("${tableName}")`).all();
    } catch (error) {
        warn(`(X) failed to inspect table '${tableName}': ${error.message}`);
        return [];
    }
}

function tableNeedsRebuild(table, tableInfo = []) {
    const existingPrimary = extractPrimaryColumns(tableInfo);
    const desiredPrimary = extractPrimaryColumnsFromSchema(table);
    if (existingPrimary.length !== desiredPrimary.length) {
        return true;
    }
    for (let index = 0; index < desiredPrimary.length; index += 1) {
        if (desiredPrimary[index] !== existingPrimary[index]) {
            return true;
        }
    }
    return false;
}

function extractPrimaryColumns(tableInfo = []) {
    return tableInfo
        .filter((row) => Number.isInteger(row?.pk) && row.pk > 0)
        .sort((a, b) => a.pk - b.pk)
        .map((row) => row.name);
}

function extractPrimaryColumnsFromSchema(table) {
    return (table?.columns ?? [])
        .filter((column) => column.primary)
        .map((column) => column.name);
}

function rebuildTable(db, table, tableInfo = []) {
    const tempName = `${table.name}__tmp_rebuild`;
    const existingColumns = tableInfo.map((row) => row.name);
    db.prepare(`DROP TABLE IF EXISTS "${tempName}"`).run();
    ensureTable(db, tempName, table.createSql);
    const copyColumns = table.columns
        .filter((column) => !column.autoIncrement && existingColumns.includes(column.name))
        .map((column) => `"${column.name}"`);
    if (copyColumns.length) {
        const columnSql = copyColumns.join(', ');
        db.prepare(`INSERT INTO "${tempName}" (${columnSql}) SELECT ${columnSql} FROM "${table.name}"`).run();
    }
    db.prepare(`DROP TABLE "${table.name}"`).run();
    db.prepare(`ALTER TABLE "${tempName}" RENAME TO "${table.name}"`).run();
    db.__contentStructureColumns.delete(tempName);
    db.__contentStructureColumns.delete(table.name);
}

function loadExistingState(db) {
    const blobHashIndex = loadExistingBlobIndex(db);
    const assetInfoKeys = loadExistingAssetInfoKeys(db);
    const imageKeys = loadExistingImageKeys(db);
    const knownBlobHashes = new Set(blobHashIndex.keys());
    const maxBlobUid = computeMaxBlobUid(blobHashIndex);
    const versionIds = loadExistingVersionIds(db);
    return {
        blobHashIndex,
        knownBlobHashes,
        assetInfoKeys,
        imageKeys,
        maxBlobUid,
        versionIds
    };
}

function loadExistingBlobIndex(db) {
    try {
        const rows = db
            .prepare('SELECT hash, blob_uid, path, size, compression, first_seen, last_seen FROM "blob_store"')
            .all();
        const map = new Map();
        for (const row of rows) {
            if (!row?.hash) {
                continue;
            }
            map.set(row.hash, {
                hash: row.hash,
                blob_uid: row.blob_uid ?? null,
                path: row.path ?? null,
                size: row.size ?? null,
                compression: row.compression ?? null,
                first_seen: row.first_seen ?? null,
                last_seen: row.last_seen ?? null
            });
        }
        return map;
    } catch (error) {
        warn(`(X) failed to load existing blob_store rows: ${error.message}`);
        return new Map();
    }
}

function computeMaxBlobUid(blobHashIndex = new Map()) {
    let maxValue = 0;
    for (const entry of blobHashIndex.values()) {
        const numeric = parseBlobUid(entry?.blob_uid);
        if (numeric > maxValue) {
            maxValue = numeric;
        }
    }
    return maxValue;
}

function parseBlobUid(blobUid) {
    if (blobUid === null || blobUid === undefined) {
        return 0;
    }
    const text = String(blobUid);
    const hex = parseInt(text, 16);
    if (Number.isFinite(hex) && hex >= 0) {
        return hex;
    }
    const decimal = parseInt(text, 10);
    if (Number.isFinite(decimal) && decimal >= 0) {
        return decimal;
    }
    return 0;
}

function loadExistingAssetInfoKeys(db) {
    try {
        const rows = db.prepare('SELECT uid, blob_uid FROM "asset_info"').all();
        const result = new Set();
        for (const row of rows) {
            result.add(buildAssetInfoKey(row?.uid, row?.blob_uid));
        }
        return result;
    } catch (error) {
        warn(`(X) failed to load existing asset_info keys: ${error.message}`);
        return new Set();
    }
}

function loadExistingImageKeys(db) {
    try {
        const rows = db.prepare('SELECT uid, blob_uid FROM "images"').all();
        const result = new Set();
        for (const row of rows) {
            result.add(buildImageKey(row?.uid, row?.blob_uid));
        }
        return result;
    } catch (error) {
        warn(`(X) failed to load existing image keys: ${error.message}`);
        return new Set();
    }
}

function loadExistingVersionIds(db) {
    try {
        const rows = db.prepare('SELECT version_id FROM "versions"').all();
        const set = new Set();
        for (const row of rows) {
            if (row?.version_id) {
                set.add(row.version_id);
            }
        }
        return set;
    } catch (error) {
        warn(`(X) failed to load existing versions: ${error.message}`);
        return new Set();
    }
}

function buildVersionRow(versionId, runDate, config, existingState) {
    if (!versionId) {
        return null;
    }
    const knownVersions = existingState?.versionIds ?? new Set();
    if (knownVersions.has(versionId)) {
        return null;
    }
    const createdAt = toIsoTimestamp(runDate);
    const type = deriveVersionType(config, existingState);
    const tags = normalizeVersionTags(config?.version_tags ?? config?.versionTags);
    return {
        version_id: versionId,
        created_at: createdAt,
        type,
        tags
    };
}

function toIsoTimestamp(value) {
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) {
        return new Date().toISOString();
    }
    return date.toISOString();
}

function deriveVersionType(config, existingState) {
    const fromConfig = normalizeVersionType(config?.version_type ?? config?.versionType);
    if (fromConfig) {
        return fromConfig;
    }
    const hasExisting = existingState?.versionIds && existingState.versionIds.size > 0;
    if (!hasExisting) {
        return 'daily';
    }
    return 'daily';
}

function normalizeVersionType(value) {
    if (!value) {
        return null;
    }
    const text = String(value).trim().toLowerCase();
    if (VERSION_TYPE_VALUES.has(text)) {
        return text;
    }
    return null;
}

function normalizeVersionTags(tags) {
    if (!tags) {
        return [];
    }
    const list = Array.isArray(tags) ? tags : String(tags).split(',');
    const normalized = [];
    for (const entry of list) {
        const text = String(entry ?? '').trim();
        if (!text) {
            continue;
        }
        normalized.push(text);
    }
    return normalized;
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
    const links = Array.isArray(content?.links) ? content.links : [];
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
    let linkCursor = 0;
    const paragraphCounters = new Map();
    let currentHeadingSlug = null;

    function pushRow({type, text, level, node, slug, assetUid, ast}) {
        const sanitizedText = typeof text === 'string' ? text : '';
        const itemOrder = orderIndex;
        const astPayload = ast !== undefined ? ast : serializeAstIfNeeded(node, type);
        rows.push({
            version_id: versionId,
            doc_sid: doc.sid,
            slug: slug ?? null,
            asset_uid: assetUid ?? null,
            type,
            level: Number.isFinite(level) ? level : null,
            order_index: itemOrder,
            body_text: sanitizedText.length ? sanitizedText : null,
            ast: astPayload
        });
        orderIndex += 1;
    }

    function recordAssetVersion(assetUid, assetType) {
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
            const typeValue = asset.type ?? assetType ?? null;
            assetVersions.push({
                asset_uid: asset.uid,
                version_id: versionId,
                doc_sid: doc.sid,
                blob_uid: asset.blob_uid ?? null,
                type: typeValue
            });
        }
        return asset;
    }

    function recordSingleAsset(assetUid, assetType) {
        const asset = recordAssetVersion(assetUid, assetType);
        return asset?.uid ?? null;
    }

    function sanitizeSlug(value) {
        const text = typeof value === 'string' ? value : '';
        const trimmed = text.trim();
        if (!trimmed) {
            return null;
        }
        return title_slug(trimmed);
    }

    function extractAssetSlug(uid) {
        if (typeof uid !== 'string') {
            return null;
        }
        if (doc?.uid) {
            const prefix = `${doc.uid}.`;
            if (uid.startsWith(prefix)) {
                const remainder = uid.slice(prefix.length);
                return remainder || null;
            }
        }
        const lastHash = uid.lastIndexOf('#');
        if (lastHash >= 0) {
            const remainder = uid.slice(lastHash + 1);
            return remainder || null;
        }
        const lastDot = uid.lastIndexOf('.');
        if (lastDot >= 0) {
            const remainder = uid.slice(lastDot + 1);
            return remainder || null;
        }
        return uid;
    }

    function buildParagraphSlug() {
        const key = currentHeadingSlug ?? '__root__';
        const next = (paragraphCounters.get(key) ?? 0) + 1;
        paragraphCounters.set(key, next);
        if (currentHeadingSlug) {
            return `${currentHeadingSlug}-p${next}`;
        }
        return `p${next}`;
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
            case 'link':
                handleLink(node);
                return;
            case 'textDirective':
                handleTextDirective(node);
                return;
            case 'containerDirective':
                handleContainerDirective(node);
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
        const slug = headingEntry?.slug ?? sanitizeSlug(text);
        currentHeadingSlug = slug ?? null;
        pushRow({
            type: 'heading',
            text,
            level,
            node,
            slug
        });
    }

    function handleParagraph(node) {
        const line = getNodeLine(node);
        const level = getLevelForLine(line);
        const segments = splitParagraphSegments(node);
        if (!segments.length) {
            const fallbackText = node_text(node) ?? '';
            if (fallbackText && fallbackText.trim()) {
                const slug = buildParagraphSlug();
                pushRow({
                    type: 'paragraph',
                    text: fallbackText,
                    level,
                    node,
                    slug
                });
            }
            return;
        }
        segments.forEach((segment) => {
                if (segment.type === 'text') {
                    if (segment.value) {
                        const slug = buildParagraphSlug();
                        pushRow({
                            type: 'paragraph',
                            text: segment.value,
                            level,
                            node: segment.node ?? node,
                            slug
                        });
                    }
                    return;
                }
                if (segment.type === 'image') {
                handleImage(segment.node, level, line);
                return;
                }
                if (segment.type === 'link') {
                    const handled = handleLink(segment.node, level, line);
                        if (!handled) {
                            const text = node_text(segment.node) ?? '';
                            if (text && text.trim()) {
                                const slug = buildParagraphSlug();
                                pushRow({
                                    type: 'paragraph',
                                    text,
                                    level,
                                    node: segment.node ?? node,
                                    slug
                                });
                            }
                        }
                        return;
            }
        });
    }

    function handleTable(node) {
        const tableEntry = tables[tableCursor++] ?? {};
        const line = getNodeLine(node);
        const level = getLevelForLine(line);
        const descriptionValue = tableEntry.text ?? tableEntry.id ?? 'table';
        const description = String(descriptionValue).trim();
        const assetSlug = extractAssetSlug(tableEntry.uid);
        const assetUid = recordSingleAsset(tableEntry.uid, 'table_data');
        const text = description || assetSlug || 'table';
        pushRow({
            type: 'table',
            text,
            level,
            node,
            slug: assetSlug,
            assetUid
        });
    }

    function handleCode(node) {
        const codeEntry = codeBlocks[codeCursor++] ?? {};
        const line = getNodeLine(node);
        const level = getLevelForLine(line);
        const language = codeEntry.language ?? node.lang ?? 'code';
        const label = `code(${language})`;
        const assetSlug = extractAssetSlug(codeEntry.uid);
        const assetUid = recordSingleAsset(codeEntry.uid, 'code_block');
        const text = label;
        const astData = {};
        if (codeEntry.meta_data && Object.keys(codeEntry.meta_data).length) {
            Object.assign(astData, codeEntry.meta_data);
        }
        if (Array.isArray(codeEntry.gallery_items) && codeEntry.gallery_items.length) {
            astData.gallery = codeEntry.gallery_items.map((entry) => ({uid: entry.uid}));
        }
        let astPayload = null;
        if (Object.keys(astData).length) {
            try {
                astPayload = JSON.stringify(astData);
            } catch (error) {
                warn(`(X) failed to serialize code ast metadata: ${error.message}`);
            }
        }
        pushRow({
            type: 'code',
            text,
            level,
            node,
            slug: assetSlug,
            assetUid,
            ast: astPayload
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
        const assetSlug = extractAssetSlug(imageEntry.uid);
        const assetUid = recordSingleAsset(imageEntry.uid, 'inline_image');
        const text = label;
        pushRow({
            type: 'image',
            text,
            level,
            node,
            slug: assetSlug,
            assetUid
        });
    }

    function handleLink(node, levelOverride, lineOverride) {
        const linkEntry = links[linkCursor++] ?? null;
        const line = lineOverride ?? getNodeLine(node);
        const level = typeof levelOverride === 'number' ? levelOverride : getLevelForLine(line);
        const assetUid = resolveLinkAssetUid(doc, linkEntry, node);
        const assetSlug = assetUid ? extractAssetSlug(assetUid) : null;
        const resolvedUid = recordSingleAsset(assetUid, 'linked_file');
        const label = formatLinkLabel(linkEntry, node) ?? assetSlug ?? 'link';
        if (label) {
            const slug = assetSlug ?? buildParagraphSlug();
            const astPayload = resolvedUid ? null : buildLinkAstPayload(node);
            pushRow({
                type: 'link',
                text: label,
                level,
                node,
                assetUid: resolvedUid ?? null,
                slug,
                ast: astPayload
            });
            return true;
        }
        return false;
    }

    function extractDirectiveAttributes(node) {
        const attributes = {};
        if (node && node.attributes && typeof node.attributes === 'object') {
            for (const [key, value] of Object.entries(node.attributes)) {
                if (value !== undefined) {
                    attributes[key] = value;
                }
            }
        }
        return attributes;
    }

    function handleTextDirective(node) {
        const line = getNodeLine(node);
        const level = getLevelForLine(line);
        const attributes = extractDirectiveAttributes(node);
        let astPayload = null;
        const payload = {name:node.name,attributes};
        if (Object.keys(attributes).length || node?.name) {
            try {
                astPayload = JSON.stringify(payload);
            } catch (error) {
                warn(`(X) failed to serialize textDirective attributes: ${error.message}`);
            }
        }
        pushRow({
            type: 'textDirective',
            text: '',
            level,
            node,
            slug: buildParagraphSlug(),
            ast: astPayload
        });
    }

    function handleContainerDirective(node) {
        const line = getNodeLine(node);
        const level = getLevelForLine(line);
        const attributes = extractDirectiveAttributes(node);
        let astPayload = null;
        const payload = {name:node.name,attributes};
        if(node.children){
            payload.children = node.children;
        }
        if (Object.keys(attributes).length || node?.name) {
            try {
                astPayload = JSON.stringify(payload);
            } catch (error) {
                warn(`(X) failed to serialize containerDirective attributes: ${error.message}`);
            }
        }
        pushRow({
            type: 'containerDirective',
            text: '',
            level,
            node,
            slug: buildParagraphSlug(),
            ast: astPayload
        });
        if (Array.isArray(node.children)) {
            node.children.forEach(processNode);
        }
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
            const children = buffer;
            buffer = [];
            const synthetic = {type: 'paragraph', children};
            const text = node_text(synthetic);
            if (text && text.trim()) {
                segments.push({type: 'text', value: text, node: synthetic});
            }
        }
        for (const child of node.children) {
            if (child.type === 'image' || child.type === 'link') {
                flushBuffer();
                segments.push({type: child.type, node: child});
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

    // Ensure document-level assets (e.g. frontmatter models) still receive a
    // version record even when they are not tied to an AST node.
    for (const asset of assets) {
        if (!asset?.uid) {
            continue;
        }
        recordAssetVersion(asset.uid, asset.type ?? null);
    }
    return {rows, assetVersions};
}

function serializeAstIfNeeded(node, itemType) {
    if (!node || !shouldStoreAstForItem(node, itemType)) {
        return null;
    }
    return serializeAstNode(node);
}

function shouldStoreAstForItem(node, itemType) {
    if (!node) {
        return false;
    }
    if (itemType === 'heading' || itemType === 'paragraph') {
        return nodeContainsMatchingDescendant(node, (child) => INLINE_COMPLEX_NODE_TYPES.has(child.type));
    }
    if (itemType === 'table') {
        return nodeContainsMatchingDescendant(node, (child) => TABLE_COMPLEX_NODE_TYPES.has(child.type));
    }
    return false;
}

function nodeContainsMatchingDescendant(node, predicate) {
    if (!node || !Array.isArray(node.children) || typeof predicate !== 'function') {
        return false;
    }
    for (const child of node.children) {
        if (predicate(child)) {
            return true;
        }
        if (nodeContainsMatchingDescendant(child, predicate)) {
            return true;
        }
    }
    return false;
}

function serializeAstNode(node) {
    try {
        const sanitized = sanitizeAstValue(node);
        return JSON.stringify(sanitized);
    } catch (error) {
        warn(`(X) failed to serialize AST node: ${error.message}`);
        return null;
    }
}

function sanitizeAstValue(value) {
    if (Array.isArray(value)) {
        return value.map((entry) => sanitizeAstValue(entry));
    }
    if (value && typeof value === 'object') {
        const result = {};
        for (const [key, child] of Object.entries(value)) {
            if (key === 'position') {
                continue;
            }
            result[key] = sanitizeAstValue(child);
        }
        return result;
    }
    return value;
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

function persistVersions(db, rows, versionsSchema, options) {
    if (!rows.length) {
        return;
    }
    const normalizedRows = rows.map((row) => normalizeTableRow(row, versionsSchema));
    insertRows(db, 'versions', versionsSchema.insertColumns, normalizedRows, options);
}

function normalizeTableRow(row, tableSchema) {
    const result = {};
    for (const column of tableSchema.columns) {
        if (column.autoIncrement) {
            continue;
        }
        result[column.name] = formatColumnValue(column, row[column.name]);
    }
    return result;
}

function buildAssetInfoKey(uid, blobUid) {
    const assetUid = uid === undefined || uid === null ? '' : String(uid);
    const blob = blobUid === undefined || blobUid === null ? '' : String(blobUid);
    return `${assetUid}|${blob}`;
}

function buildImageKey(uid, blobUid) {
    const imageUid = uid === undefined || uid === null ? '' : String(uid);
    const blob = blobUid === undefined || blobUid === null ? '' : String(blobUid);
    return `${imageUid}|${blob}`;
}

function filterNewAssetInfoRows(assets, existingState) {
    if (!assets?.length) {
        return [];
    }
    const keys = existingState?.assetInfoKeys ?? new Set();
    const rows = [];
    for (const asset of assets) {
        if (!asset?.uid) {
            continue;
        }
        const key = buildAssetInfoKey(asset.uid, asset.blob_uid);
        if (keys.has(key)) {
            continue;
        }
        keys.add(key);
        rows.push(asset);
    }
    return rows;
}

function filterNewBlobRows(blobs, existingState) {
    if (!blobs?.length) {
        return [];
    }
    const knownHashes = existingState?.knownBlobHashes ?? new Set();
    const hashIndex = existingState?.blobHashIndex ?? new Map();
    const rows = [];
    for (const blob of blobs) {
        const hash = blob?.hash;
        if (!hash) {
            continue;
        }
        if (knownHashes.has(hash)) {
            const existing = hashIndex.get(hash);
            if (existing && blob.blob_uid === undefined) {
                blob.blob_uid = existing.blob_uid ?? blob.blob_uid ?? null;
            }
            continue;
        }
        knownHashes.add(hash);
        hashIndex.set(hash, blob);
        rows.push(blob);
    }
    return rows;
}

function filterNewImages(images, existingState) {
    if (!images?.length) {
        return [];
    }
    const keys = existingState?.imageKeys ?? new Set();
    const rows = [];
    for (const image of images) {
        if (!image?.uid) {
            continue;
        }
        const key = buildImageKey(image.uid, image.blob_uid);
        if (keys.has(key)) {
            continue;
        }
        keys.add(key);
        rows.push(image);
    }
    return rows;
}

function persistAssetInfo(db, assets, assetsSchema, existingState, options) {
    const filteredAssets = filterNewAssetInfoRows(assets, existingState);
    if (!filteredAssets.length) {
        return;
    }
    const rows = filteredAssets.map((asset) => ({
        uid: asset.uid,
        type: asset.type ?? null,
        blob_uid: asset.blob_uid ?? null,
        parent_doc_uid: asset.parent_doc_uid ?? null,
        path: asset.path ?? null,
        ext: asset.ext ?? null,
        params: asset.params ?? null,
        meta_data: asset.meta_data ?? null,
        first_seen: asset.first_seen ?? null,
        last_seen: asset.last_seen ?? null
    }));
    insertRows(db, 'asset_info', assetsSchema.insertColumns, rows, options);
}

function persistBlobStore(db, blobs, blobsSchema, existingState, options) {
    const filteredBlobs = filterNewBlobRows(blobs, existingState);
    if (!filteredBlobs.length) {
        return;
    }
    const rows = filteredBlobs.map((blob) => ({
        blob_uid: blob.blob_uid ?? null,
        hash: blob.hash ?? null,
        size: blob.size ?? null,
        path: blob.path ?? null,
        payload: blob.payload ?? null,
        compression: normalizeScalar(blob.compression),
        first_seen: blob.first_seen ?? null,
        last_seen: blob.last_seen ?? null
    }));
    insertRows(db, 'blob_store', blobsSchema.insertColumns, rows, options);
}

function persistImages(db, images, imagesSchema, existingState, options) {
    const filteredImages = filterNewImages(images, existingState);
    if (!filteredImages.length) {
        return;
    }
    const rows = filteredImages.map((image) => ({
        uid: image.uid ?? null,
        blob_uid: image.blob_uid ?? null,
        type: image.type ?? null,
        name: image.name ?? null,
        extension: image.extension ?? null,
        width: image.width ?? null,
        height: image.height ?? null,
        ratio: image.ratio ?? null
    }));
    insertRows(db, 'images', imagesSchema.insertColumns, rows, options);
}

function resolveLinkAssetUid(doc, linkEntry, node) {
    if (!doc?.uid) {
        return null;
    }
    if (linkEntry?.id) {
        return `${doc.uid}.link-${linkEntry.id}`;
    }
    const hashId = buildLinkHashId(node);
    if (!hashId) {
        return null;
    }
    return `${doc.uid}.${hashId}`;
}

function buildLinkHashId(node) {
    const raw = node?.url;
    if (typeof raw !== 'string' || raw.trim().length === 0) {
        return null;
    }
    return `link-${shortMD5(raw)}`;
}

function formatLinkLabel(linkEntry, node) {
    const text = typeof linkEntry?.text === 'string' ? linkEntry.text.trim() : '';
    if (text) {
        return text;
    }
    const title = typeof linkEntry?.title === 'string' ? linkEntry.title.trim() : '';
    if (title) {
        return title;
    }
    const fallback = node_text(node) ?? '';
    const trimmed = fallback.trim();
    return trimmed.length ? trimmed : null;
}

function buildLinkAstPayload(node) {
    if (!node) {
        return null;
    }
    const url = typeof node.url === 'string' ? node.url : null;
    const title = typeof node.title === 'string' ? node.title : null;
    const payload = {title, url};
    try {
        return JSON.stringify(payload);
    } catch (error) {
        warn(`(X) failed to serialize link metadata: ${error.message}`);
        return null;
    }
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
