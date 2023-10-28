import { save_json } from './src/utils.js';
import {parse_documents,collect_documents,
        get_all_md_files, set_config} from './src/collect.js'

async function collect(config){
    set_config(config)
    const files_paths = await get_all_md_files()
    if(config.debug){
        console.log(files_paths)
    }
    const documents = await collect_documents(files_paths)
    console.log("index.json")
    await save_json(documents,"index.json")
    
    await parse_documents(documents)//generates for each document content.json,tree.json
}

export{
    collect
}
