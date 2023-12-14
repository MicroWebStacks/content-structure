import {join} from 'path'
import { save_json,load_json,check_dir_create } from './src/utils.js';
import {get_images_info,get_codes_info,get_tables_info,
        get_links_info,get_refs_info} from './src/md_utils.js'
import {parse_document,collect_documents_data,
        get_all_files, set_config,parse_markdown,
        check_add_assets} from './src/collect.js'
import { debug,green_log } from './src/libs/log.js';

async function collect(config){

    //enforced matches for internal cross referencing
    if(!Object.hasOwn(config,"matches")){
        config.matches = {}
    }
    config.matches.page = 'page::([\\w-.]+)'
    config.matches.sid = 'sid::([\\w-.]+)'

    set_config(config)
    const files_paths = await get_all_files(config.content_ext)
    const documents = await collect_documents_data(files_paths)
    debug(files_paths)

    const asset_list = []
    const reference_list = []
    for(const entry of documents){
        if(entry.format == "markdown"){
            debug(`   parsing entry.path = ${entry.path}`)
            const {tree,content} = await parse_document(entry)
            const dir = join("documents",entry.sid)
            await check_dir_create(dir)
            asset_list.push(...get_images_info(entry,content))
            asset_list.push(...get_tables_info(entry,content))
            asset_list.push(...get_codes_info(entry,content))
            asset_list.push(...get_links_info(entry,content,config.assets_ext))
            reference_list.push(...get_refs_info(entry,content))
            await save_json(tree,join(dir,"tree.json"))
            await save_json(content,join(dir,"content.json"))
        }
    }
    const content_assets = await get_all_files(config.assets_ext)
    await check_add_assets(asset_list,content_assets)

    await check_dir_create("")//even root dir might need creation
    await save_json(documents,"document_list.json")
    green_log(`saved document_list.json with ${documents.length} documents`)

    await save_json(asset_list,"asset_list.json")
    green_log(`saved asset_list.json with ${asset_list.length} assets`)
    await save_json(reference_list,"reference_list.json")
    green_log(`saved reference_list.json with ${reference_list.length} references`)
}

function filter_documents(data,filterCriteria) {
    return data.filter(entry => {
        return Object.entries(filterCriteria).every(([key, value]) => {
        return entry[key] === value;
        });
    });
}

async function getDocuments(filter= null){
    const documents = await load_json("document_list.json","output")
    if(filter == null){
        return documents
    }else{
        return filter_documents(documents,filter)
    }
}

async function getEntry(filter){
    const documents = await load_json("document_list.json","output")
    const filetred_documents = filter_documents(documents,filter)
    if(filetred_documents.length == 0){
        console.warn(` X entry not found '${JSON.stringify(filter)}'`)
    }else{
        if(filetred_documents.length != 1){
            console.warn(` X more than one document found, returning first for : '${JSON.stringify(filter)}'`)
        }
        const entry_data = filetred_documents[0]
        const data = await load_json(join("documents",entry_data.sid,"content.json"),"output")
        const tree = await load_json(join("documents",entry_data.sid,"tree.json"),"output")
        return {tree,data}
    }
    return {tree:{},data:{}}
}

export{
    collect,
    getDocuments,
    getEntry,
    set_config,
    parse_markdown
}
