import { join } from 'path'
import { exists, exists_public, file_ext } from './src/utils.js';
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
    const blobState = createBlobState()

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
            const {tree,content,assets:documentAssets} = await tree_content(markdownText,entryDetails)
            const assetList = []
            if(modelAsset){
                assetList.push(modelAsset)
            }
            if(Array.isArray(documentAssets) && documentAssets.length > 0){
                assetList.push(...documentAssets)
            }

            await annotateAssets(assetList,config)
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
                    path:result.path ?? null
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
                        path:result.path ?? null
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
    if(timestamp){
        if(!entry.first_seen){
            entry.first_seen = timestamp
        }
        entry.last_seen = timestamp
    }
    catalog.set(data.hash,entry)
    return entry
}


export{
    collect,
    set_config
}
