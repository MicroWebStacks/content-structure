import {join} from 'path'
import { save_json,load_json,check_dir_create } from './src/utils.js';
import {parse_document,collect_documents_data,
        get_all_files, set_config} from './src/collect.js'

async function collect(config){
    set_config(config)
    const files_paths = await get_all_files(["md","json","yml","yaml"])
    if(config.debug){
        console.log(files_paths)
    }
    const documents = await collect_documents_data(files_paths)
    console.log("index.json")
    await save_json(documents,"index.json")

    for(const entry of documents){
        if(entry.format == "markdown"){
            const {tree,content} = await parse_document(entry)
            const dir = join("documents",entry.sid)
            console.log(dir)
            await check_dir_create(dir)
            await save_json(tree,join(dir,"tree.json"))
            await save_json(content,join(dir,"content.json"))
        }
    }

}

function filter_documents(data,filterCriteria) {
    return data.filter(entry => {
        return Object.entries(filterCriteria).every(([key, value]) => {
        return entry[key] === value;
        });
    });
}

async function get_documents(filter= null){
    const documents = await load_json("index.json","output")
    if(filter == null){
        return documents
    }else{
        return filter_documents(documents,filter)
    }
}

export{
    collect,
    get_documents
}
