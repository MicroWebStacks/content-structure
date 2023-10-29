import {join} from 'path'
import { save_json,check_dir_create } from './src/utils.js';
import {parse_document,collect_documents,
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

    for(const entry of documents){
        const {tree,content} = await parse_document(entry)
        const dir = join("documents",entry.sid)
        console.log(dir)
        await check_dir_create(dir)
        await save_json(tree,join(dir,"tree.json"))
        await save_json(content,join(dir,"content.json"))
    }

}

export{
    collect
}
