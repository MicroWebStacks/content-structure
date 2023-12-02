import {glob} from 'glob'
import { relative, resolve, join, sep, basename, dirname, parse, extname } from 'path';
import path from 'path';
import { get_next_uid, load_yaml, load_json, load_text } from './utils.js';
import { md_tree, title_slug, extract_headings,
        extract_tables,extract_images,extract_code,
        extract_paragraphs, extract_tags } from './md_utils.js';
import matter from 'gray-matter';
import { createHash } from 'crypto';


let config = {
    rootdir: process.cwd(),
    rel_contentdir: "content",
    rel_outdir: "gen",
    debug:false
}

/**
 * type in data         => priority for type from data
 * depth > 1            => auto-type from parent folder
 * root content         => generic
 */
function get_type(data,file_path,url_type){
    if(Object.hasOwn("type")){
        return data.type
    }else{
        let depth = file_path.split('/').length
        let parent_path = dirname(file_path)
        if(url_type == "dir"){
            depth -= 1
            parent_path = dirname(parent_path)
        }
        if(depth > 1){
            return basename(parent_path)
        }else{
            return "generic"
        }
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
    let uid = (type === "generic") ? slug : `${type}.${slug}`
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

async function get_all_files(ext_list){
    const content_dir = join(config.rootdir,config.rel_contentdir);
    console.log(`content_dir : ${content_dir}`)
    const originalDirectory = process.cwd();
    process.chdir(content_dir)
    const filter = ext_list.map((ext)=>`*.${ext}`).join(",")
    const results = await glob(content_dir+`/**/{${filter}}`)
    //change to abs then rel to be cross os compatible
    const files = results.map((file)=>(relative(content_dir,resolve(content_dir,file)).split(sep).join('/')))
    console.log(`change back to originalDirectory : ${originalDirectory}`)
    process.chdir(originalDirectory)
    return files
}

function entry_to_url(url_type,path,slug){
    if(url_type == "dir"){
        const dir = dirname(dirname(path))
        return join(dir, slug).replaceAll('\\','/')
    }else{
        const parsedPath = parse(path)
        return join(parsedPath.dir, slug).replaceAll('\\','/')
    }
}

async function get_markdown_data(file_path){
    const url_type = (file_path.endsWith("readme.md")?"dir":"file")
    const text = await load_text(file_path)
    const {content, data} = matter(text)

    const slug = get_slug(data,file_path,url_type)
    const content_type = get_type(data,file_path,url_type)
    const uid = get_uid(slug,content_type)
    const sid = get_sid(uid)
    const title = Object.hasOwn(data,"title")?data.title:slug
    if(Object.hasOwn(data,"title")){
        delete data.title
    }
    const url = entry_to_url(url_type,file_path,slug)
    let entry       = {
        sid:            sid,         //short unique id
        uid:            uid,        //unique, fallback appending -1, -2,...
        path:           file_path,
        url:            url,
        url_type:       url_type,
        slug:           slug,       //not unique
        format:         "markdown",
        title:          title,
        content_type:   content_type,
        ...data,
    }
    return entry
}

async function get_data(file_path){
    const parsedPath = path.parse(file_path);
    const file_base_name = parsedPath.name
    const url_type = (["entry","document"].includes(file_base_name)?"dir":"file")
    const extension = parsedPath.ext
    let data = {}
    if(extension == ".json"){
        data = await load_json(file_path)
    }else{
        data = await load_yaml(file_path)
    }
    const slug = get_slug(data,file_path,url_type)
    const content_type = get_type(data,file_path,url_type)
    const uid = get_uid(slug,content_type)
    const sid = get_sid(uid)
    const url = entry_to_url(url_type,file_path,slug)
    let entry       = {
        sid:            sid,         //short unique id
        uid:            uid,        //unique, fallback appending -1, -2,...
        path:           file_path,
        url:            url,
        url_type:       url_type,
        slug:           slug,       //not unique
        format:         "data",
        content_type:   content_type,
        ...data,
    }
    return entry
}

async function collect_documents_data(files_paths){
    let content_entries = []
    for(const file_path  of files_paths){
        const extension = extname(file_path)
        if(extension == ".md"){
            const entry = await get_markdown_data(file_path)
            content_entries.push(entry)
        }else if((extension == ".yaml")||(extension == ".yml")||(extension == ".json")){
            const entry = await get_data(file_path)
            content_entries.push(entry)
        }
    }
    return content_entries
}

async function parse_markdown(markdown,path){
    const entry_details = {}
    const tree = md_tree(markdown)
    
    const headings = extract_headings(tree)
    entry_details.headings = headings
    const tables = extract_tables(tree,headings)
    entry_details.tables = tables
    const images = await extract_images(tree,headings,dirname(path))
    entry_details.images = images
    const code = extract_code(tree,headings)
    entry_details.code = code
    const paragraphs = extract_paragraphs(tree,headings)
    entry_details.paragraphs = paragraphs
    const tags = extract_tags(tree,headings)
    entry_details.tags = tags

    return {tree,content:entry_details}
}

async function parse_document(entry){
    const entry_details = JSON.parse(JSON.stringify(entry))
    const text = await load_text(entry.path)

    const {content, data} = matter(text)
    const tree = md_tree(content)
    
    const headings = extract_headings(tree)
    entry_details.headings = headings
    const tables = extract_tables(tree,headings)
    entry_details.tables = tables
    const images = await extract_images(tree,headings,dirname(entry.path))
    entry_details.images = images
    const code = extract_code(tree,headings)
    entry_details.code = code
    const paragraphs = extract_paragraphs(tree,headings)
    entry_details.paragraphs = paragraphs
    const tags = extract_tags(tree,headings)
    entry_details.tags = tags

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
    collect_documents_data,
    get_all_files,
    set_config,
    get_config,
    parse_markdown
}
