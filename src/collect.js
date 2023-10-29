import {glob} from 'glob'
import { relative, resolve, join, sep, basename, dirname, parse } from 'path';
import {promises as fs} from 'fs';
import { check_dir_create,save_json, get_next_uid } from './utils.js';
import { md_tree, title_slug, extract_headings,
        extract_tables,extract_images,extract_code,
        extract_paragraphs } from './md_utils.js';
import matter from 'gray-matter';
import { createHash } from 'crypto';

let config = {
    rootdir: process.cwd(),
    rel_contentdir: "content",
    rel_outdir: "gen",
    debug:false
}

function get_type(data){
    if(Object.hasOwn("type")){
        return data.type
    }else{
        return "generic"
    }
}

function get_slug(data,path,url_type){
    if(Object.hasOwn(data,"slug")){
        return data.slug
    }else if(Object.hasOwn(data,"title")){
        return title_slug(data.title)
    }else if(url_type == "dir"){
        return basename(dirname(path))
    }else{
        return parse(path).name
    }
}

let content_urls = new Map()

function get_uid(slug,type){
    let uid = (type === "generic") ? slug : `${type}/${slug}`
    if(!content_urls.has(type)){
        content_urls.set(type,[uid])    //create new list
    }else{
        uid = get_next_uid(uid,content_urls.get(type))
        content_urls.get(type).push(uid)
    }
    return uid
}

function get_sid(uid){
    const hash = createHash('md5')
    hash.update(uid)
    return hash.digest('hex').slice(0,8)
}

async function get_all_md_files(){
    const content_dir = join(config.rootdir,config.rel_contentdir);
    console.log(`content_dir : ${content_dir}`)
    const originalDirectory = process.cwd();
    process.chdir(content_dir)
    const results = await glob(content_dir+"/**/*.md")
    //change to abs then rel to be cross os compatible
    const files = results.map((file)=>(relative(content_dir,resolve(content_dir,file)).split(sep).join('/')))
    console.log(`change back to originalDirectory : ${originalDirectory}`)
    process.chdir(originalDirectory)
    return files
}

async function collect_documents(files_paths){
    let content_entries = []
    for(const file_path  of files_paths){
        const url_type = (file_path.endsWith("readme.md")?"dir":"file")
        const abs_file_path = join(config.rootdir,config.rel_contentdir,file_path)
        const text = await fs.readFile(abs_file_path,'utf-8')
        const {content, data} = matter(text)
        const slug = get_slug(data,file_path,url_type)
        const content_type = get_type(data)
        const uid = get_uid(slug,content_type)
        const sid = get_sid(uid)
        const title = Object.hasOwn(data,"title")?data.title:slug
        if(Object.hasOwn(data,"title")){
            delete data.title
        }
        let entry       = {
            title:          title,
            ...data,
            path:           file_path,
            content_type:   content_type,
            url_type:       url_type,
            slug:           slug,       //not unique
            uid:            uid,        //unique, fallback appending -1, -2,...
            sid:            sid         //short unique id
        }

        content_entries.push(entry)
    }
    return content_entries
}

async function parse_document(entry){
    const entry_details = JSON.parse(JSON.stringify(entry))
    const abs_file_path = join(config.rootdir,config.rel_contentdir,entry.path)
    const text = await fs.readFile(abs_file_path,'utf-8')
    const {content, data} = matter(text)
    const tree = md_tree(content)

    const headings = extract_headings(tree)
    entry_details.headings = headings
    const tables = extract_tables(tree,headings)
    entry_details.tables = tables
    const images = await extract_images(tree,headings,dirname(abs_file_path))
    entry_details.images = images
    const code = extract_code(tree,headings)
    entry_details.code = code
    const paragraphs = extract_paragraphs(tree,headings)
    entry_details.paragraphs = paragraphs
    return {tree,content:entry_details}
}

function set_config(new_config){
    if(new_config != null){
        config = new_config
        if(config.debug){
            console.log("config:")
            console.log(config)
        }
    }else{
        console.warn("config not provided, using:")
        console.log(config)
    }
}

function get_config(){
    return config
}

export{
    parse_document,
    collect_documents,
    get_all_md_files,
    set_config,
    get_config
}
