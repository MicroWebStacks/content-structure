import { join } from 'path';
import { check_dir_create } from './utils.js';
import { get_config, shortMD5 } from './collect.js';
import { warn } from './libs/log.js';
import { openDatabase, ensureTable, clearTable, ensureColumn, insertRows, runInTransaction } from './sqlite_utils/index.js';

const DOCUMENT_TABLE_SQL = `
    sid TEXT PRIMARY KEY,
    uid TEXT NOT NULL,
    path TEXT NOT NULL,
    url TEXT,
    url_type TEXT,
    slug TEXT,
    format TEXT,
    title TEXT,
    content_type TEXT,
    level INTEGER,
    headings_list TEXT NOT NULL,
    links_list TEXT NOT NULL,
    references_list TEXT NOT NULL,
    image_sid_list TEXT NOT NULL,
    table_sid_list TEXT NOT NULL,
    code_sid_list TEXT NOT NULL,
    paragraph_sid_list TEXT NOT NULL
`;

const ASSET_TABLE_SQL = `
    sid TEXT PRIMARY KEY,
    uid TEXT NOT NULL,
    type TEXT NOT NULL,
    document_sid TEXT,
    path TEXT,
    url TEXT,
    text TEXT,
    external INTEGER,
    ext TEXT,
    filter_ext INTEGER,
    "exists" INTEGER,
    abs_path TEXT,
    hash TEXT,
    language TEXT
`;

const TABLES_TABLE_SQL = `
    sid TEXT PRIMARY KEY,
    uid TEXT NOT NULL,
    document_sid TEXT NOT NULL,
    heading TEXT,
    text TEXT,
    data_list TEXT NOT NULL,
    order_index INTEGER
`;

const IMAGES_TABLE_SQL = `
    sid TEXT PRIMARY KEY,
    uid TEXT NOT NULL,
    document_sid TEXT NOT NULL,
    slug TEXT,
    source_sid TEXT,
    heading TEXT,
    title TEXT,
    alt TEXT,
    url TEXT,
    text_list TEXT NOT NULL,
    references_list TEXT NOT NULL,
    order_index INTEGER
`;

const CODE_TABLE_SQL = `
    sid TEXT PRIMARY KEY,
    uid TEXT NOT NULL,
    document_sid TEXT NOT NULL,
    heading TEXT,
    language TEXT,
    text TEXT,
    hash TEXT,
    order_index INTEGER
`;

const PARAGRAPH_TABLE_SQL = `
    sid TEXT PRIMARY KEY,
    uid TEXT NOT NULL,
    document_sid TEXT NOT NULL,
    heading TEXT,
    text TEXT,
    order_index INTEGER
`;

const REFERENCES_TABLE_SQL = `
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type TEXT,
    source_sid TEXT,
    source_heading TEXT,
    target_type TEXT,
    target_uid TEXT,
    target_sid TEXT
`;

const DOCUMENT_BASE_FIELDS = new Set([
    'sid',
    'uid',
    'path',
    'url',
    'url_type',
    'slug',
    'format',
    'title',
    'content_type',
    'level',
    'headings_list',
    'links_list',
    'references_list',
    'image_sid_list',
    'table_sid_list',
    'code_sid_list',
    'paragraph_sid_list'
]);

const DOCUMENT_BASE_COLUMN_ORDER = [
    'sid',
    'uid',
    'path',
    'url',
    'url_type',
    'slug',
    'format',
    'title',
    'content_type',
    'level',
    'headings_list',
    'links_list',
    'references_list',
    'image_sid_list',
    'table_sid_list',
    'code_sid_list',
    'paragraph_sid_list'
];

const DB_FILENAME = 'structure.db';

async function writeStructureDb({documents, assets, references, documentContents}) {
    const config = get_config();
    await check_dir_create('');
    const dbPath = join(config.outdir, DB_FILENAME);
    let db;
    try {
        db = openDatabase(dbPath);
    } catch (error) {
        warn(`(!) skipping structure.db generation: ${error.message}`);
        return;
    }
    runInTransaction(db, () => {
        createTables(db);
        resetTables(db);
        const contentMap = normalizeContentMap(documentContents);
        const {docRows, tableRows, imageRows, codeRows, paragraphRows} = buildDocumentPayloads(documents, contentMap);
        persistDocuments(db, docRows);
        persistSimpleRows(db, 'tables', ['sid', 'uid', 'document_sid', 'heading', 'text', 'data_list', 'order_index'], tableRows);
        persistSimpleRows(db, 'images', ['sid', 'uid', 'document_sid', 'slug', 'source_sid', 'heading', 'title', 'alt', 'url', 'text_list', 'references_list', 'order_index'], imageRows);
        persistSimpleRows(db, 'code', ['sid', 'uid', 'document_sid', 'heading', 'language', 'text', 'hash', 'order_index'], codeRows);
        persistSimpleRows(db, 'paragraphs', ['sid', 'uid', 'document_sid', 'heading', 'text', 'order_index'], paragraphRows);
        persistAssets(db, assets ?? []);
        persistReferences(db, references ?? []);
    });
}

function createTables(db) {
    ensureTable(db, 'documents', DOCUMENT_TABLE_SQL);
    ensureTable(db, 'assets', ASSET_TABLE_SQL);
    ensureTable(db, 'tables', TABLES_TABLE_SQL);
    ensureTable(db, 'images', IMAGES_TABLE_SQL);
    ensureTable(db, 'code', CODE_TABLE_SQL);
    ensureTable(db, 'paragraphs', PARAGRAPH_TABLE_SQL);
    ensureTable(db, 'references', REFERENCES_TABLE_SQL);
}

function resetTables(db) {
    clearTable(db, 'documents');
    clearTable(db, 'assets');
    clearTable(db, 'tables');
    clearTable(db, 'images');
    clearTable(db, 'code');
    clearTable(db, 'paragraphs');
    clearTable(db, 'references');
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

function buildDocumentPayloads(documents, contentMap) {
    const docRows = [];
    const tableRows = [];
    const imageRows = [];
    const codeRows = [];
    const paragraphRows = [];
    for (const doc of documents) {
        const content = contentMap.get(doc.sid);
        const {row, tables, images, code, paragraphs} = buildDocumentRow(doc, content);
        docRows.push(row);
        tableRows.push(...tables);
        imageRows.push(...images);
        codeRows.push(...code);
        paragraphRows.push(...paragraphs);
    }
    return {docRows, tableRows, imageRows, codeRows, paragraphRows};
}

function buildDocumentRow(doc, content) {
    const row = {};
    for (const column of DOCUMENT_BASE_COLUMN_ORDER) {
        if (column === 'level') {
            row[column] = doc.level ?? null;
        } else {
            row[column] = doc[column] ?? null;
        }
    }
    appendDocumentExtras(doc, row);
    const tablesResult = buildTableRows(doc, content?.tables ?? []);
    const imagesResult = buildImageRows(doc, content?.images ?? []);
    const codeResult = buildCodeRows(doc, content?.code ?? []);
    const paragraphsResult = buildParagraphRows(doc, content?.paragraphs ?? []);
    row.headings_list = serializeList(content?.headings ?? []);
    row.links_list = serializeList(content?.links ?? []);
    row.references_list = serializeList(content?.references ?? []);
    row.image_sid_list = serializeList(imagesResult.sids);
    row.table_sid_list = serializeList(tablesResult.sids);
    row.code_sid_list = serializeList(codeResult.sids);
    row.paragraph_sid_list = serializeList(paragraphsResult.sids);
    return {
        row,
        tables: tablesResult.rows,
        images: imagesResult.rows,
        code: codeResult.rows,
        paragraphs: paragraphsResult.rows
    };
}

function appendDocumentExtras(doc, row) {
    for (const [key, value] of Object.entries(doc)) {
        if (DOCUMENT_BASE_FIELDS.has(key) || value === undefined) {
            continue;
        }
        if (Array.isArray(value)) {
            const columnName = key.endsWith('_list') ? key : `${key}_list`;
            row[columnName] = serializeList(value);
        } else if (value !== null && typeof value === 'object') {
            const columnName = key.endsWith('_list') ? key : `${key}_list`;
            row[columnName] = serializeList([value]);
        } else {
            row[key] = normalizeScalar(value);
        }
    }
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

function persistDocuments(db, rows) {
    if (!rows.length) {
        return;
    }
    const columnSet = new Set(DOCUMENT_BASE_COLUMN_ORDER);
    for (const row of rows) {
        for (const key of Object.keys(row)) {
            if (!columnSet.has(key)) {
                ensureColumn(db, 'documents', key, 'TEXT');
                columnSet.add(key);
            }
        }
    }
    insertRows(db, 'documents', Array.from(columnSet), rows);
}

function persistSimpleRows(db, tableName, columns, rows) {
    if (!rows.length) {
        return;
    }
    insertRows(db, tableName, columns, rows);
}

function persistAssets(db, assets) {
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
    insertRows(db, 'assets', ['sid', 'uid', 'type', 'document_sid', 'path', 'url', 'text', 'external', 'ext', 'filter_ext', 'exists', 'abs_path', 'hash', 'language'], rows);
}

function persistReferences(db, references) {
    if (!references.length) {
        return;
    }
    insertRows(
        db,
        'references',
        ['source_type', 'source_sid', 'source_heading', 'target_type', 'target_uid', 'target_sid'],
        references
    );
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
    writeStructureDb
};
