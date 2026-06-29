// JSON structure writer.
//
// Mirrors the createStructureDbWriter interface (existingState + insertDocument
// / insertAssets / insertBlobs / insertImages) but accumulates rows in memory
// and flushes a self-contained JSON dataset in finalize():
//
//   <json_dir>/content.json     { version_id, diagram, documents[], items[],
//                                 asset_info[], assets[], images[], blob_store[] }
//   <json_dir>/blobs/<blob_uid> raw, already-decompressed blob bytes
//
// It reuses the SAME pure row-builders as the SQLite writer (buildDocumentRow /
// getStructureSchema), so there is no duplication of the mdast -> rows logic and
// no native dependency (better-sqlite3 / sharp are never imported here).
import { join } from 'path';
import { mkdir, writeFile, rm } from 'fs/promises';
import { get_config } from './collect.js';
import { computeVersionId } from './version_id.js';
import { getStructureSchema, buildDocumentRow } from './structure_db.js';
import { writeBlobFiles } from './blob_files.js';
import { warn } from './libs/log.js';

async function createStructureJsonWriter(options = {}) {
    const config = get_config();
    const versionId = options?.versionId ?? computeVersionId();
    const schema = await getStructureSchema();
    const documentsSchema = schema.tables.get('documents');
    if (!documentsSchema) {
        warn('(!) skipping json generation: catalog.yaml missing documents table');
        return null;
    }

    const documents = [];
    const items = [];
    const assetVersions = [];
    const assetInfo = [];
    const blobs = [];
    const images = [];

    const outdir = config.outdir;
    const jsonDir = config.json_dir ?? join(outdir ?? '.', 'json');
    const blobsDir = join(jsonDir, 'blobs');

    // A fresh JSON export is non-incremental: no prior state to dedup against.
    const existingState = {
        blobHashIndex: new Map(),
        knownBlobHashes: new Set(),
        assetInfoKeys: new Set(),
        imageKeys: new Set(),
        maxBlobUid: 0,
        versionIds: new Set()
    };

    return {
        existingState,
        insertDocument(entry, content, tree, assets) {
            if (!entry) {
                return;
            }
            const payload = buildDocumentRow(entry, content, documentsSchema, {versionId, tree, assets});
            documents.push(payload.row);
            if (payload.items?.length) {
                items.push(...payload.items);
            }
            if (payload.assetVersions?.length) {
                assetVersions.push(...payload.assetVersions);
            }
        },
        insertAssets(list = []) {
            for (const asset of list) {
                if (!asset?.uid) {
                    continue;
                }
                assetInfo.push({
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
                });
            }
        },
        insertBlobs(list = []) {
            for (const blob of list) {
                blobs.push({
                    blob_uid: blob.blob_uid ?? null,
                    hash: blob.hash ?? null,
                    size: blob.size ?? null,
                    path: blob.path ?? null,
                    payload: blob.payload ?? null,
                    compression: blob.compression ?? null
                });
            }
        },
        insertImages(list = []) {
            for (const image of list) {
                images.push({
                    uid: image.uid ?? null,
                    blob_uid: image.blob_uid ?? null,
                    type: image.type ?? null,
                    name: image.name ?? null,
                    extension: image.extension ?? null,
                    width: image.width ?? null,
                    height: image.height ?? null,
                    ratio: image.ratio ?? null
                });
            }
        },
        async finalize() {
            await rm(blobsDir, {recursive: true, force: true});
            await mkdir(blobsDir, {recursive: true});

            // Content-addressed static files: <hash>.<ext>, one per (hash,ext) an
            // asset references, so the URL resolver can map any asset to a stable,
            // immutable, browser-cacheable path. Bytes resolved + decompressed.
            const blobByUid = new Map();
            for (const blob of blobs) {
                if (blob.blob_uid != null) {
                    blobByUid.set(String(blob.blob_uid), blob);
                }
            }
            const refs = [];
            for (const asset of assetInfo) {
                refs.push({blob: blobByUid.get(String(asset.blob_uid)), ext: asset.ext});
            }
            for (const image of images) {
                refs.push({blob: blobByUid.get(String(image.blob_uid)), ext: image.extension});
            }
            const {count, missing} = await writeBlobFiles(refs, blobsDir, outdir);

            // Blob bytes live on disk (content-addressed); keep only metadata.
            const blobMeta = blobs.map((blob) => ({
                blob_uid: blob.blob_uid,
                hash: blob.hash,
                size: blob.size,
                path: blob.path,
                compression: blob.compression
            }));

            const dataset = {
                version_id: versionId,
                diagram: config.diagram ?? null,
                documents,
                items,
                asset_info: assetInfo,
                assets: assetVersions,
                images,
                blob_store: blobMeta
            };
            await writeFile(join(jsonDir, 'content.json'), JSON.stringify(dataset));

            console.log(
                `content-structure(json): version ${versionId} -> ${jsonDir}\n` +
                    `  documents=${documents.length} items=${items.length} ` +
                    `asset_info=${assetInfo.length} assets=${assetVersions.length} images=${images.length}\n` +
                    `  blob files written=${count}${missing ? ` (missing=${missing})` : ''}`
            );
        }
    };
}

export {
    createStructureJsonWriter
};
