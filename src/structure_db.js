import { join } from 'path';
import { check_dir_create, load_yaml_code } from './utils.js';
import { get_config, shortMD5 } from './collect.js';
import { warn } from './libs/log.js';
import { openDatabase, ensureTable, clearTable, insertRows, runInTransaction } from './sqlite_utils/index.js';

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

async function createStructureDbWriter() {
    const config = get_config();
    await check_dir_create('');
    const dbPath = join(config.outdir, DB_FILENAME);
    const schema = await getStructureSchema();
    let db;
    try {
        db = openDatabase(dbPath);
    } catch (error) {
        warn(`(!) skipping structure.db generation: ${error.message}`);
        return null;
    }
    const documentsSchema = requireTableSchema(schema, 'documents');
    const tablesSchema = requireTableSchema(schema, 'tables');
    const imagesSchema = requireTableSchema(schema, 'images');
    const codeSchema = requireTableSchema(schema, 'code');
    const paragraphsSchema = requireTableSchema(schema, 'paragraphs');
    const assetsSchema = requireTableSchema(schema, 'assets');
    const referencesSchema = requireTableSchema(schema, 'references');
    runInTransaction(db, () => {
        createTables(db, schema);
        resetTables(db, schema);
    });
    const insertDocumentTx = db.transaction((payload) => {
        const {row, tables, images, code, paragraphs} = payload;
        persistDocuments(db, [row], documentsSchema, {transaction: false});
        persistSimpleRows(db, 'tables', tablesSchema.insertColumns, tables, {transaction: false});
        persistSimpleRows(db, 'images', imagesSchema.insertColumns, images, {transaction: false});
        persistSimpleRows(db, 'code', codeSchema.insertColumns, code, {transaction: false});
        persistSimpleRows(db, 'paragraphs', paragraphsSchema.insertColumns, paragraphs, {transaction: false});
    });
    return {
        insertDocument(entry, content) {
            if (!entry) {
                return;
            }
            const payload = buildDocumentRow(entry, content, documentsSchema);
            insertDocumentTx(payload);
        },
        insertAssets(assetsList = []) {
            persistAssets(db, assetsList, assetsSchema);
        },
        insertReferences(refList = []) {
            persistReferences(db, refList, referencesSchema);
        }
    };
}

async function writeStructureDb({documents = [], assets = [], references = [], documentContents}) {
    const writer = await createStructureDbWriter();
    if (!writer) {
        return;
    }
    const contentMap = normalizeContentMap(documentContents);
    for (const doc of documents) {
        const content = contentMap.get(doc.sid);
        writer.insertDocument(doc, content);
    }
    if (assets.length) {
        writer.insertAssets(assets);
    }
    if (references.length) {
        writer.insertReferences(references);
    }
}

function createTables(db, schema) {
    for (const table of schema.tables.values()) {
        ensureTable(db, table.name, table.createSql);
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

function buildDocumentPayloads(documents, contentMap, documentSchema) {
    const docRows = [];
    const tableRows = [];
    const imageRows = [];
    const codeRows = [];
    const paragraphRows = [];
    for (const doc of documents) {
        const content = contentMap.get(doc.sid);
        const {row, tables, images, code, paragraphs} = buildDocumentRow(doc, content, documentSchema);
        docRows.push(row);
        tableRows.push(...tables);
        imageRows.push(...images);
        codeRows.push(...code);
        paragraphRows.push(...paragraphs);
    }
    return {docRows, tableRows, imageRows, codeRows, paragraphRows};
}

function buildDocumentRow(doc, content, documentSchema) {
    const tablesResult = buildTableRows(doc, content?.tables ?? []);
    const imagesResult = buildImageRows(doc, content?.images ?? []);
    const codeResult = buildCodeRows(doc, content?.code ?? []);
    const paragraphsResult = buildParagraphRows(doc, content?.paragraphs ?? []);
    const row = {};
    for (const column of documentSchema.columns) {
        row[column.name] = getDocumentColumnValue(column, doc, content, {
            tablesResult,
            imagesResult,
            codeResult,
            paragraphsResult
        });
    }
    return {
        row,
        tables: tablesResult.rows,
        images: imagesResult.rows,
        code: codeResult.rows,
        paragraphs: paragraphsResult.rows
    };
}

function getDocumentColumnValue(column, doc, content, derived) {
    switch (column.name) {
        case 'headings_list':
            return formatColumnValue(column, content?.headings ?? []);
        case 'links_list':
            return formatColumnValue(column, content?.links ?? []);
        case 'references_list':
            return formatColumnValue(column, content?.references ?? []);
        case 'image_sid_list':
            return formatColumnValue(column, derived.imagesResult.sids);
        case 'table_sid_list':
            return formatColumnValue(column, derived.tablesResult.sids);
        case 'code_sid_list':
            return formatColumnValue(column, derived.codeResult.sids);
        case 'paragraph_sid_list':
            return formatColumnValue(column, derived.paragraphsResult.sids);
        default:
            return formatColumnValue(column, doc[column.name]);
    }
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

function buildTableRows(doc, tables) {
    const rows = [];
    const sids = [];
    tables.forEach((table, index) => {
        sids.push(table.sid);
        rows.push({
            sid: table.sid,
            uid: table.uid,
            document_sid: doc.sid,
            heading: table.heading ?? null,
            text: table.text ?? null,
            data_list: serializeList(table.data ?? []),
            order_index: index
        });
    });
    return {rows, sids};
}

function buildImageRows(doc, images) {
    const rows = [];
    const sids = [];
    images.forEach((image, index) => {
        const baseId = image.id ?? `image-${index + 1}`;
        const uid = `${doc.uid}@${baseId}`;
        const sid = shortMD5(uid);
        sids.push(sid);
        rows.push({
            sid,
            uid,
            document_sid: doc.sid,
            slug: baseId,
            source_sid: image.sid ?? null,
            heading: image.heading ?? null,
            title: image.title ?? null,
            alt: image.alt ?? null,
            url: image.url ?? null,
            text_list: serializeList(image.text_list ?? []),
            references_list: serializeList(image.references ?? []),
            order_index: index
        });
    });
    return {rows, sids};
}

function buildCodeRows(doc, codeBlocks) {
    const rows = [];
    const sids = [];
    codeBlocks.forEach((codeBlock, index) => {
        sids.push(codeBlock.sid);
        rows.push({
            sid: codeBlock.sid,
            uid: codeBlock.uid,
            document_sid: doc.sid,
            heading: codeBlock.heading ?? null,
            language: codeBlock.language ?? null,
            text: codeBlock.text ?? '',
            hash: shortMD5(codeBlock.text ?? ''),
            order_index: index
        });
    });
    return {rows, sids};
}

function buildParagraphRows(doc, paragraphs) {
    const rows = [];
    const sids = [];
    paragraphs.forEach((paragraph, index) => {
        const uid = `${doc.uid}@paragraph-${index + 1}`;
        const sid = shortMD5(uid);
        sids.push(sid);
        rows.push({
            sid,
            uid,
            document_sid: doc.sid,
            heading: paragraph.heading ?? null,
            text: paragraph.text ?? '',
            order_index: index
        });
    });
    return {rows, sids};
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
        sid: asset.sid,
        uid: asset.uid,
        type: asset.type,
        document_sid: asset.document ?? null,
        path: asset.path ?? null,
        url: asset.url ?? null,
        text: asset.text ?? null,
        external: normalizeScalar(asset.external),
        ext: asset.ext ?? null,
        filter_ext: normalizeScalar(asset.filter_ext),
        exists: normalizeScalar(asset.exists),
        abs_path: asset.abs_path ?? null,
        hash: asset.hash ?? null,
        language: asset.language ?? null
    }));
    insertRows(db, 'assets', assetsSchema.insertColumns, rows, options);
}

function persistReferences(db, references, referencesSchema, options) {
    if (!references.length) {
        return;
    }
    insertRows(db, 'references', referencesSchema.insertColumns, references, options);
}

function serializeList(list) {
    const value = Array.isArray(list) ? list : [];
    return JSON.stringify(value);
}

function normalizeScalar(value) {
    if (value === null || value === undefined) {
        return null;
    }
    if (typeof value === 'boolean') {
        return value ? 1 : 0;
    }
    return value;
}

export {
    writeStructureDb,
    createStructureDbWriter
};
