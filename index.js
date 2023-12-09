import {join} from 'path'
import { save_json,load_json,check_dir_create } from './src/utils.js';
import {get_images_info,get_codes_info} from './src/md_utils.js'
import {parse_document,collect_documents_data,
        get_all_files, set_config,parse_markdown} from './src/collect.js'

async function collect(config){
    set_config(config)
    const files_paths = await get_all_files(config.extensions)
    if(config.debug){
        console.log(files_paths)
    }
    const documents = await collect_documents_data(files_paths)
    console.log("document_list.json")
    await check_dir_create("")//even root dir might need creation
    await save_json(documents,"document_list.json")

    const image_list = []
    const code_list = []
    for(const entry of documents){
        if(entry.format == "markdown"){
            const {tree,content} = await parse_document(entry)
            const dir = join("documents",entry.sid)
            console.log(entry.path)
            await check_dir_create(dir)
            await save_json(tree,join(dir,"tree.json"))
            await save_json(content,join(dir,"content.json"))
            image_list.push(...get_images_info(entry,content))
            code_list.push(...get_codes_info(entry,content))
        }
    }
    await save_json(image_list,"image_list.json")
    await save_json(code_list,"code_list.json")
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
