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
    const {all_images} = await parse_documents(documents)
    const content = {
        documents,
        images:all_images
    }
    
    console.log("index.json")
    await save_json(content,"index.json")
}

export{
    collect
}
