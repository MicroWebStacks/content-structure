import { join } from 'path'
import { exists, exists_public, file_ext } from './src/utils.js';
import {get_images_info,get_codes_info,get_tables_info,get_links_info} from './src/md_utils.js'
import {iterate_documents, set_config, tree_content} from './src/collect.js'
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
    const blobCatalog = new Map()

    const originalCwd = process.cwd()
    try{
        process.chdir(config.contentdir)
        for await (const source of iterate_documents()){
            const {entry, markdownText, modelAsset} = source ?? {}
            if(!entry){
                continue
            }
            documentIndex[entry.sid] = {
                type:"document",
                uid:entry.uid
            }
            debug(` parsing sid: ${entry.sid} path: ${entry.path}`)
            const entryDetails = cloneEntry(entry)
            const {tree,content} = await tree_content(markdownText,entryDetails)
            const assetList = []
            if(modelAsset){
                assetList.push(modelAsset)
            }
            const imageAssets = await get_images_info(entry,content)
            const tableAssets = get_tables_info(entry,content)
            const codeAssets = get_codes_info(entry,content)
            const linkAssets = await get_links_info(entry,content)
            assetList.push(...imageAssets,...tableAssets,...codeAssets,...linkAssets)

            await annotateAssets(assetList,config)
            stampAssets(assetList, runTimestamp)
            await attachBlobsToAssets(assetList, blobManager, blobCatalog, runTimestamp)
            writer.insertDocument(entry,content,tree,assetList)
            if(assetList.length > 0){
                writer.insertAssets(assetList)
                addAssetsToIndex(assetIndex,assetList)
            }
        }
        if(blobCatalog.size > 0){
            writer.insertBlobs(Array.from(blobCatalog.values()))
        }
    }finally{
        process.chdir(originalCwd)
    }

}

async function annotateAssets(assets,config){
    for(const asset of assets){
        if(!Object.hasOwn(asset,"path")){
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

async function attachBlobsToAssets(assets,blobManager,blobCatalog = new Map(),timestamp){
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
                recordBlobMetadata(blobCatalog,{
                    hash:result.hash,
                    size:result.size ?? buffer.length,
                    path:result.path ?? null
                },timestamp)
            }
            continue
        }
        if(asset.abs_path){
            try{
                const result = await blobManager.ensureFromFile(asset.abs_path)
                if(result){
                    asset.blob_hash = result.hash
                    recordBlobMetadata(blobCatalog,{
                        hash:result.hash,
                        size:result.size ?? null,
                        path:result.path ?? null
                    },timestamp)
                }
            }catch(error){
                warn(`(X) failed to create blob for '${asset.path}': ${error.message}`)
            }
        }
    }
}

function recordBlobMetadata(blobCatalog,data,timestamp){
    if(!data || !data.hash){
        return
    }
    const existing = blobCatalog.get(data.hash) ?? {hash:data.hash}
    if(Number.isFinite(data.size)){
        existing.size = data.size
    }
    if(data.path){
        existing.path = data.path
    }
    if(timestamp){
        if(!existing.first_seen){
            existing.first_seen = timestamp
        }
        existing.last_seen = timestamp
    }
    blobCatalog.set(data.hash,existing)
}


export{
    collect,
    set_config
}
