import {glob} from 'glob'
import { relative, resolve, join, sep, basename, dirname, parse, extname } from 'path';
import path from 'path';
import { get_next_uid, load_yaml, load_json, load_text,exists,exists_public } from './utils.js';
import { md_tree, title_slug, extract_headings,
        extract_tables,extract_images,extract_code,
        extract_paragraphs, extract_links,extract_refs } from './md_utils.js';
import matter from 'gray-matter';
import { createHash } from 'crypto';
import {warn, debug} from './libs/log.js'
import {textListMatches} from './node-text-matches.js'

let config = {
    rootdir: process.cwd(),
    contentdir: "content",
    outdir: ".structure",
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

function shortMD5(text) {
    const hash = createHash('md5').update(text, 'utf8').digest('hex');
    return hash.substring(0, 8);
}
  
  
async function get_all_files(ext_list){
    const content_dir = join(config.contentdir);
    console.log(`content_dir : ${content_dir}`)
    const originalDirectory = process.cwd();
    process.chdir(content_dir)
    const filter = ext_list.map((ext)=>`*.${ext}`).join(",")
    console.log(`   searching for files with extensions : ${filter}`)
    let glob_query = content_dir+`/**/{${filter}}`
    if(ext_list.length == 1){
        glob_query = content_dir+`/**/${filter}`
    }
    const results = await glob(glob_query)
    //change to abs then rel to be cross os compatible
    const files = results.map((file)=>(relative(content_dir,resolve(content_dir,file)).split(sep).join('/')))
    debug(`change back to originalDirectory : ${originalDirectory}`)
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
    const sid = shortMD5(uid)
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
    const sid = shortMD5(uid)
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

async function tree_content(markdown_text,entry_details){
    const config = get_config()
    const {content, data} = matter(markdown_text)
    const tree = md_tree(content)
    const headings = extract_headings(tree)
    entry_details.headings = headings
    const tables = extract_tables(tree,headings,entry_details)
    entry_details.tables = tables
    const images = await extract_images(tree,headings,entry_details)
    for(const image of images){
        image.references = textListMatches(image.text_list,config.matches)
    }
    entry_details.images = images
    const code = extract_code(tree,headings,entry_details)
    entry_details.code = code
    const paragraphs = extract_paragraphs(tree,headings)
    entry_details.paragraphs = paragraphs
    const links = extract_links(tree,headings)
    entry_details.links = links
    const references = extract_refs(tree,headings)
    entry_details.references = references

    return {tree,content:entry_details}
}

//unused internally, exported service
async function parse_markdown(markdown,path){
    const entry_details = {
        path:path,
        uid:path,//for images uid assignments
        sid:shortMD5(path)
    }
    return tree_content(markdown,entry_details)
}

async function parse_document(entry){
    const entry_details = JSON.parse(JSON.stringify(entry))
    const markdown_text = await load_text(entry.path)
    return tree_content(markdown_text,entry_details)
}

async function check_add_assets(asset_list,content_assets){
    const referenced_locals = new Set()
    for(const asset of asset_list){
        if(Object.hasOwn(asset,"path")){
            referenced_locals.add(asset.path)
            if(asset.path.startsWith("/")){
                if(!await exists_public(asset.path)){
                    warn(`(X) asset does not exist in public '${asset.path}'`)
                }
            }else if(!await exists(asset.path)){
                warn(`(X) asset does not exist in content '${asset.path}'`)
            }
        }
    }
    for(const filepath of content_assets){
        if(!referenced_locals.has(filepath)){
            const uid = filepath.replaceAll("/",".")
            asset_list.push({
                type:"found",
                uid:uid,
                sid:shortMD5(uid),
                path:filepath
            })
        }
    }
}

function set_config(new_config){
    if(new_config != null){
        config = new_config
        if(config.debug){
            console.log("config:")
            console.log(config)
        }
    }else{
        warn("config not provided, using:")
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
    check_add_assets,
    set_config,
    get_config,
    parse_markdown,
    shortMD5
}
