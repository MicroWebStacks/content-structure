import { join, dirname, parse } from 'path'
import sharp from 'sharp';
import { exists, exists_public, file_ext, exists_abs } from './src/utils.js';
import {iterate_documents, set_config, tree_content, shortMD5} from './src/collect.js'
import { debug, warn } from './src/libs/log.js';
import { createStructureDbWriter } from './src/structure_db.js';
import { createBlobManager } from './src/blob_manager.js';
import { computeVersionId } from './src/version_id.js';

function decodePathValue(pathValue){
    if(!pathValue){
        return pathValue
    }
    try{
        return decodeURIComponent(pathValue)
    }catch(error){
        warn(`(X) failed to decode path '${pathValue}': ${error.message}`)
        return pathValue
    }
}

function cloneEntry(entry){
    return JSON.parse(JSON.stringify(entry))
}

async function collect(config){

    set_config(config)
    const runDate = new Date()
    const runTimestamp = runDate.toISOString()
    const versionId = computeVersionId(runDate)
    const writer = await createStructureDbWriter({versionId})
    if(!writer){
        return
    }

    const assetIndex = Object.create(null)
    const documentIndex = Object.create(null)
    const blobManager = createBlobManager(runTimestamp)
    const blobState = createBlobState()
    const orderTracker = createDocumentOrderTracker()
    const imageCatalog = createImageCatalog()

    const originalCwd = process.cwd()
    try{
        process.chdir(config.contentdir)
        for await (const source of iterate_documents()){
            const {entry, markdownText} = source ?? {}
            if(!entry){
                continue
            }
            assignDocumentOrder(entry, orderTracker)
            if(entry.version_id === undefined || entry.version_id === null){
                entry.version_id = versionId
            }
            documentIndex[entry.sid] = {
                type:"document",
                uid:entry.uid
            }
            debug(` parsing sid: ${entry.sid} path: ${entry.path}`)
            const entryDetails = cloneEntry(entry)
            const {tree,content,assets:documentAssets} = await tree_content(markdownText,entryDetails)
            const assetList = []
            if(Array.isArray(documentAssets) && documentAssets.length > 0){
                assetList.push(...documentAssets)
            }
            await ensureFrontmatterImageAsset(entry, content, assetList)

            await annotateAssets(assetList,config)
            await collectImageMetadata(assetList, imageCatalog)
            stampAssets(assetList, runTimestamp)
            await attachBlobsToAssets(assetList, blobManager, blobState, runTimestamp)
            writer.insertDocument(entry,content,tree,assetList)
            if(assetList.length > 0){
                writer.insertAssets(assetList)
                addAssetsToIndex(assetIndex,assetList)
            }
        }
        if(blobState.catalog.size > 0){
            writer.insertBlobs(Array.from(blobState.catalog.values()))
        }
        if(imageCatalog.size > 0){
            writer.insertImages(Array.from(imageCatalog.values()))
        }
    }finally{
        process.chdir(originalCwd)
    }

}

async function annotateAssets(assets,config){
    for(const asset of assets){
        if(!asset || !Object.hasOwn(asset,"path")){
            continue
        }
        if(asset.abs_path){
            asset.abs_path = decodePathValue(asset.abs_path)
            if(!asset.ext && asset.path){
                asset.ext = file_ext(asset.path)
            }
            if(!Object.hasOwn(asset,'exists')){
                asset.exists = true
            }
            continue
        }
        let asset_exist = false
        let abs_path = ""
        if(asset.path.startsWith("/")){
            if(await exists_public(asset.path)){
                asset_exist = true
                abs_path = join(config.rootdir,"public",asset.path)
            }
        }else{
            if(await exists(asset.path)){
                asset_exist = true
                abs_path = join(config.contentdir,asset.path)
            }
        }
        if(asset_exist){
            asset.exists = asset_exist
            asset.abs_path = decodePathValue(abs_path)
            if(!asset.ext){
                asset.ext = file_ext(asset.path)
            }
        }
    }
}

function addAssetsToIndex(index,assets){
    for(const asset of assets){
        index[asset.sid] = {
            type:asset.type,
            uid:asset.uid
        }
    }
}

function stampAssets(assets,timestamp){
    if(!timestamp){
        return
    }
    for(const asset of assets){
        if(!asset){
            continue
        }
        if(!asset.first_seen){
            asset.first_seen = timestamp
        }
        asset.last_seen = timestamp
    }
}

async function attachBlobsToAssets(assets,blobManager,blobState,timestamp){
    if(!blobManager || !blobState){
        return
    }
    for(const asset of assets){
        if(!asset || asset.blob_uid){
            continue
        }
        if(typeof asset.blob_content === 'string'){
            const buffer = Buffer.from(asset.blob_content,'utf8')
            const result = await blobManager.ensureFromBuffer(buffer)
            if(result){
                const blobEntry = registerBlob(blobState,{
                    hash:result.hash,
                    size:result.size ?? buffer.length,
                    path:result.path ?? null,
                    payload:result.payload ?? null,
                    compression:result.compression ?? null
                },timestamp)
                if(blobEntry){
                    asset.blob_uid = blobEntry.blob_uid
                }
            }
            continue
        }
        if(asset.abs_path){
            try{
                const result = await blobManager.ensureFromFile(asset.abs_path)
                if(result){
                    const blobEntry = registerBlob(blobState,{
                        hash:result.hash,
                        size:result.size ?? null,
                        path:result.path ?? null,
                        payload:result.payload ?? null,
                        compression:result.compression ?? null
                    },timestamp)
                    if(blobEntry){
                        asset.blob_uid = blobEntry.blob_uid
                    }
                }
            }catch(error){
                warn(`(X) failed to create blob for '${asset.path}': ${error.message}`)
            }
        }
    }
}

function createBlobState(){
    return {
        catalog:new Map(),
        counter:0
    }
}

function registerBlob(blobState,data,timestamp){
    if(!blobState || !data || !data.hash){
        return null
    }
    const {catalog} = blobState
    let entry = catalog.get(data.hash)
    if(!entry){
        blobState.counter += 1
        entry = {
            blob_uid: blobState.counter.toString(16),
            hash:data.hash
        }
    }
    if(Number.isFinite(data.size)){
        entry.size = data.size
    }
    if(data.path){
        entry.path = data.path
    }
    if(Object.hasOwn(data,'payload') && data.payload !== null && data.payload !== undefined){
        entry.payload = data.payload
    }
    if(Object.hasOwn(data,'compression')){
        if(data.compression === null || data.compression === undefined){
            if(!Object.hasOwn(entry,'compression')){
                entry.compression = null
            }
        }else{
            entry.compression = data.compression
        }
    }
    if(timestamp){
        if(!entry.first_seen){
            entry.first_seen = timestamp
        }
        entry.last_seen = timestamp
    }
    catalog.set(data.hash,entry)
    return entry
}

function createDocumentOrderTracker(){
    return new Map()
}

function assignDocumentOrder(entry, tracker){
    if(!entry || !tracker){
        return
    }
    const key = buildOrderGroupKey(entry)
    let group = tracker.get(key)
    if(!group){
        group = {
            taken:new Set(),
            next:1
        }
        tracker.set(key,group)
    }
    const explicitOrder = normalizeOrderValue(entry.order)
    if(explicitOrder !== null){
        recordOrderValue(group, explicitOrder)
        entry.order = explicitOrder
        return
    }
    let candidate = group.next
    while(group.taken.has(candidate)){
        candidate += 1
    }
    recordOrderValue(group, candidate)
    entry.order = candidate
}

function normalizeOrderValue(value){
    if(value === null || value === undefined){
        return null
    }
    const numberValue = Number(value)
    if(!Number.isFinite(numberValue)){
        return null
    }
    const normalized = Math.floor(numberValue)
    if(normalized <= 0){
        return null
    }
    return normalized
}

function recordOrderValue(group, value){
    group.taken.add(value)
    if(value === group.next){
        while(group.taken.has(group.next)){
            group.next += 1
        }
    }
}

function buildOrderGroupKey(entry){
    const baseDir = typeof entry?.base_dir === 'string' && entry.base_dir.length > 0
        ? entry.base_dir
        : '.'
    const level = Number.isFinite(entry?.level) ? entry.level : 0
    return `${baseDir}|${level}`
}

function parseMetaObject(raw){
    if(typeof raw !== 'string' || raw.trim().length === 0){
        return {}
    }
    try{
        const parsed = JSON.parse(raw)
        if(parsed && typeof parsed === 'object' && !Array.isArray(parsed)){
            return parsed
        }
    }catch(_error){
        /* ignore malformed meta */
    }
    return {}
}

function isExternalPath(value){
    if(!value){
        return true
    }
    const trimmed = value.trim()
    if(!trimmed){
        return true
    }
    if(trimmed.startsWith('asset:///')){
        return true
    }
    if(trimmed.startsWith('//')){
        return true
    }
    return /^[a-zA-Z][a-zA-Z\d+\-.]*:/.test(trimmed)
}

function normalizeRelativePath(pathValue){
    if(!pathValue){
        return null
    }
    return pathValue.replace(/^[.][\\/]/,'').replaceAll('\\','/')
}

function resolveEntryAssetPath(entry, target){
    if(!target){
        return null
    }
    const baseDir = typeof entry?.base_dir === 'string' && entry.base_dir.length > 0
        ? entry.base_dir
        : dirname(entry?.path ?? '')
    if(!baseDir && target.startsWith('/')){
        return null
    }
    if(target.startsWith('/')){
        return target
    }
    return normalizeRelativePath(join(baseDir || '', target))
}

async function ensureFrontmatterImageAsset(entry, content, assetList){
    if(!entry){
        return
    }
    const metaSource = entry.meta_data ?? content?.meta_data
    const meta = parseMetaObject(metaSource)
    const imageValue = meta.image
    if(typeof imageValue !== 'string' || imageValue.trim().length === 0){
        return
    }
    if(isExternalPath(imageValue)){
        return
    }
    const trimmed = imageValue.trim()
    const existingUid = assetList.find((asset)=>asset?.uid === trimmed)
    if(existingUid){
        return
    }
    const resolved = resolveEntryAssetPath(entry, trimmed)
    if(!resolved || resolved.startsWith('/')){
        return
    }
    const cleanedPath = normalizeRelativePath(resolved)
    if(!cleanedPath){
        return
    }
    let asset = assetList.find((entryAsset)=>entryAsset?.path === cleanedPath)
    if(!asset){
        const existsLocally = await exists(cleanedPath)
        if(!existsLocally){
            return
        }
        const ext = file_ext(cleanedPath)
        const slugBase = `meta-image-${shortMD5(`${entry.uid}:${cleanedPath}`)}${ext ? `.${ext.toLowerCase()}` : ''}`
        const uid = `${entry.uid}.${slugBase}`
        asset = {
            type:'image',
            uid,
            sid:shortMD5(uid),
            document:entry.sid,
            parent_doc_uid:entry.uid,
            path:cleanedPath,
            ext:ext ?? null
        }
        assetList.push(asset)
    }
    meta.image = asset.uid
    const serialized = JSON.stringify(meta)
    entry.meta_data = serialized
    if(content){
        content.meta_data = serialized
    }
}

function createImageCatalog(){
    return new Map()
}

async function collectImageMetadata(assets, imageCatalog){
    for(const asset of assets){
        if(!asset || !asset.uid){
            continue
        }
        if(asset.type !== 'image' && asset.type !== 'gallery_asset'){
            continue
        }
        if(imageCatalog.has(asset.uid)){
            continue
        }
        const absPath = asset.abs_path ?? null
        if(!absPath){
            continue
        }
        const existsOnDisk = await exists_abs(absPath)
        if(!existsOnDisk){
            continue
        }
        let metadata
        try{
            metadata = await sharp(absPath).metadata()
        }catch(error){
            warn(`(X) failed to read image metadata '${asset.path ?? asset.uid}': ${error.message}`)
            continue
        }
        let width = metadata?.width ?? null
        let height = metadata?.height ?? null
        const orientation = metadata?.orientation
        if(Number.isInteger(orientation) && orientation >= 5 && orientation <= 8){
            const swapped = width
            width = height
            height = swapped
        }
        if(!Number.isFinite(width) || !Number.isFinite(height) || height === 0){
            continue
        }
        const ratio = width / height
        const extension = deriveImageExtension(asset)
        const name = deriveImageName(asset)
        imageCatalog.set(asset.uid,{
            uid:asset.uid,
            type:asset.type ?? null,
            name:name ?? null,
            extension:extension ?? null,
            width:Math.round(width),
            height:Math.round(height),
            ratio:ratio
        })
    }
}

function deriveImageExtension(asset){
    if(asset?.ext){
        return String(asset.ext).toLowerCase()
    }
    const pathValue = asset?.path ?? ''
    const ext = file_ext(pathValue)
    return ext ? ext.toLowerCase() : null
}

function deriveImageName(asset){
    const pathValue = asset?.path ?? ''
    if(pathValue){
        const parsed = parse(pathValue)
        if(parsed.name){
            return parsed.name
        }
    }
    if(asset?.abs_path){
        const parsedAbs = parse(asset.abs_path)
        if(parsedAbs.name){
            return parsedAbs.name
        }
    }
    return null
}

export{
    collect,
    set_config
}
