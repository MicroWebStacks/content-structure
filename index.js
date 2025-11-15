import {join} from 'path'
import { save_json,load_json,check_dir_create, list_to_map,add_documents } from './src/utils.js';
import {get_images_info,get_codes_info,get_tables_info,
        get_links_assets_info,get_refs_info} from './src/md_utils.js'
import {parse_document,collect_documents_data,
        get_all_files, set_config,parse_markdown,
        check_add_assets} from './src/collect.js'
import { debug,green_log } from './src/libs/log.js';
import { writeStructureDb } from './src/structure_db.js';

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
    const documentContents = new Map()

    const asset_list = []
    await check_dir_create("ast")
    for(const entry of documents){
        if(entry.format.startsWith("markdown")){
            debug(` parsing sid: ${entry.sid} path: ${entry.path}`)
            const {tree,content} = await parse_document(entry)
            documentContents.set(entry.sid, content)
            asset_list.push(...get_images_info(entry,content))
            asset_list.push(...get_tables_info(entry,content))
            asset_list.push(...get_codes_info(entry,content))
            asset_list.push(...get_links_assets_info(entry,content,config.assets_ext))
            await save_json(tree,join("ast",`${entry.sid}.json`))
            entry.references = content.references
            entry.images = content.images
        }
    }
    const content_assets = await get_all_files(config.assets_ext)
    await check_add_assets(asset_list,content_assets)
    
    const asset_map = list_to_map(asset_list,"sid")
    const all_items_map = add_documents(asset_map,documents)
    const reference_list = []
    for(const entry of documents){
        if(entry.format.startsWith("markdown")){
            reference_list.push(...get_refs_info(entry,all_items_map))
            delete entry.references
            delete entry.images
        }
    }

    await writeStructureDb({
        documents,
        assets: asset_list,
        references: reference_list,
        documentContents
    })
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
        const tree = await load_json(join("ast",`${entry_data.sid}.json`),"output")
        return {tree}
    }
    return {tree:{}}
}

export{
    collect,
    getDocuments,
    getEntry,
    set_config,
    parse_markdown
}
