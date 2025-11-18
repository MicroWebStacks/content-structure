import {join} from 'path'
import { save_json,check_dir_create, exists, exists_public, file_ext } from './src/utils.js';
import {get_images_info,get_codes_info,get_tables_info,
        get_refs_info} from './src/md_utils.js'
import {parse_document,collect_document_data,
        get_all_files, set_config,parse_markdown,
        shortMD5} from './src/collect.js'
import { debug, warn } from './src/libs/log.js';
import { createStructureDbWriter } from './src/structure_db.js';
import { createBlobManager } from './src/blob_manager.js';

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

async function collect(config){

    //enforced matches for internal cross referencing
    if(!Object.hasOwn(config,"matches")){
        config.matches = {}
    }
    config.matches.page = 'page::([\\w-.]+)'
    config.matches.sid = 'sid::([\\w-.]+)'

    set_config(config)
    const writer = await createStructureDbWriter()
    if(!writer){
        return
    }

    const assetIndex = Object.create(null)
    const documentIndex = Object.create(null)
    const referenceSources = []
    const referencedLocalAssets = new Set()
    const runTimestamp = new Date().toISOString()
    const blobManager = createBlobManager(runTimestamp)

    await check_dir_create("ast")
    const originalCwd = process.cwd()
    try{
        process.chdir(config.contentdir)
        for await (const file_path of get_all_files(config.content_ext)){
            const entry = await collect_document_data(file_path)
            if(entry == null){
                continue
            }
            documentIndex[entry.sid] = {
                type:"document",
                uid:entry.uid
            }
            if(entry.format.startsWith("markdown")){
                debug(` parsing sid: ${entry.sid} path: ${entry.path}`)
                const {tree,content} = await parse_document(entry)
                await save_json(tree,join("ast",`${entry.sid}.json`))
                const documentAssets = [
                    ...get_images_info(entry,content),
                    ...get_tables_info(entry,content),
                    ...get_codes_info(entry,content)
                ]
                await annotateAssets(documentAssets,config,referencedLocalAssets)
                stampAssets(documentAssets, runTimestamp)
                await attachBlobsToAssets(documentAssets, blobManager)
                writer.insertDocument(entry,content)
                if(documentAssets.length > 0){
                    writer.insertAssets(documentAssets)
                    addAssetsToIndex(assetIndex,documentAssets)
                }
                referenceSources.push(buildReferenceSource(entry,content))
            }else{
                writer.insertDocument(entry)
            }
        }
    }finally{
        process.chdir(originalCwd)
    }

    const foundAssets = await collectUnreferencedAssets(config,referencedLocalAssets)
    if(foundAssets.length > 0){
        await annotateAssets(foundAssets,config,null)
        stampAssets(foundAssets, runTimestamp)
        await attachBlobsToAssets(foundAssets, blobManager)
        writer.insertAssets(foundAssets)
        addAssetsToIndex(assetIndex,foundAssets)
    }

    const reference_list = buildReferenceList(referenceSources,assetIndex,documentIndex)
    if(reference_list.length > 0){
        writer.insertReferences(reference_list)
    }
    const blobRows = blobManager.getRows()
    if(blobRows.length > 0){
        writer.insertBlobs(blobRows)
    }
}

async function annotateAssets(assets,config,referencedLocalAssets){
    for(const asset of assets){
        if(!Object.hasOwn(asset,"path")){
            continue
        }
        if(referencedLocalAssets){
            referencedLocalAssets.add(asset.path)
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
        }else if(asset.filter_ext){
            asset.exists = asset_exist
            warn(`(X) asset from filter ext does not exist '${asset.path}'`)
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

function buildReferenceSource(entry,content){
    return {
        sid:entry.sid,
        uid:entry.uid,
        references:content.references ?? [],
        images:(content.images ?? []).map((image)=>({
            sid:image.sid,
            heading:image.heading,
            references:image.references ?? []
        }))
    }
}

async function collectUnreferencedAssets(config,referencedLocalAssets){
    const assets = []
    for await (const filepath of get_all_files(config.assets_ext)){
        if(referencedLocalAssets.has(filepath)){
            continue
        }
        const uid = filepath.replaceAll("/",".")
        assets.push({
            type:"found",
            uid:uid,
            sid:shortMD5(uid),
            path:filepath,
            parent_doc_uid:null,
            ext:file_ext(filepath)
        })
    }
    return assets
}

function buildReferenceList(referenceSources,assetIndex,documentIndex){
    const allItemsMap = {...assetIndex,...documentIndex}
    const references = []
    for(const entry of referenceSources){
        references.push(...get_refs_info(entry,allItemsMap))
    }
    return references
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

async function attachBlobsToAssets(assets,blobManager){
    if(!blobManager){
        return
    }
    for(const asset of assets){
        if(!asset || asset.blob_hash){
            continue
        }
        if(typeof asset.blob_content === 'string'){
            const buffer = Buffer.from(asset.blob_content,'utf8')
            const result = await blobManager.ensureFromBuffer(buffer)
            if(result){
                asset.blob_hash = result.hash
            }
            continue
        }
        if(asset.abs_path){
            try{
                const result = await blobManager.ensureFromFile(asset.abs_path)
                if(result){
                    asset.blob_hash = result.hash
                }
            }catch(error){
                warn(`(X) failed to create blob for '${asset.path}': ${error.message}`)
            }
        }
    }
}


export{
    collect,
    set_config,
    parse_markdown
}
