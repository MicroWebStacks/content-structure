import {globStream} from 'glob'
import { relative, resolve, join, sep, basename, dirname, parse, extname } from 'path';
import { load_text,exists,exists_public } from './utils.js';
import { md_tree, title_slug, extract_headings,
        extract_tables,extract_images,extract_code,
        extract_paragraphs, extract_links,extract_refs } from './md_utils.js';
import matter from 'gray-matter';
import { createHash } from 'crypto';
import {warn, debug} from './libs/log.js'

let config = {
    rootdir: process.cwd(),
    contentdir: "content",
    outdir: ".structure",
    debug:false,
    folder_single_doc:false
}

const KNOWN_ENTRY_FIELDS = new Set([
    'title','slug','order','description','date','lastmod','image','tags','features'
])


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

function buildDocumentUid(urlPath, slug, fallbackPath){
    const normalizedUrl = normalizeUrlToUid(urlPath);
    if(normalizedUrl){
        return normalizedUrl;
    }
    if(slug){
        return slug.replaceAll('/', '.');
    }
    const sanitizedPath = (fallbackPath ?? '').replace(/\.[^.]+$/, '').split('/').filter(Boolean).join('.');
    if(sanitizedPath){
        return sanitizedPath;
    }
    return shortMD5(fallbackPath ?? '');
}

function normalizeUrlToUid(urlPath){
    if(!urlPath){
        return null;
    }
    const segments = urlPath.split('/').filter(Boolean);
    if(segments.length === 0){
        return null;
    }
    return segments.join('.');
}

function shortMD5(text) {
    const hash = createHash('md5').update(text, 'utf8').digest('hex');
    return hash.substring(0, 8);
}
  
  
async function* get_all_files(ext_list){
    console.log(`content_dir : ${config.contentdir}`)
    const filter = ext_list.map((ext)=>`*.${ext}`).join(",")
    console.log(`   searching for files with extensions : ${filter}`)
    const globPattern = (ext_list.length === 1)
        ? `**/${filter}`
        : `**/{${filter}}`
    const stream = globStream(globPattern,{
        cwd: config.contentdir,
        absolute: true,
        nodir: true
    })
    for await (const filePath of stream){
        const normalized = relative(config.contentdir,resolve(filePath)).split(sep).join('/')
        yield normalized
    }
}

function entry_to_url(url_type,path,slug){
    if(url_type == "dir"){
        const dir = dirname(path)
        if(dir == "."){
            return ""
        }
        return dir.replaceAll('\\','/')
    }else{
        const parsedPath = parse(path)
        return join(parsedPath.dir, slug).replaceAll('\\','/')
    }
}

function entry_to_level(url_type,file_path){
    const base_level = 1
    let level = 1
    const directory = dirname(file_path)
    if(![".",""].includes(directory)){
        //console.log(directory.split('/'))
        const path_level = directory.split('/').length
        if(url_type == "file"){
            level = base_level + path_level + 1
        }else{
            level = base_level + path_level
        }
    }
    //console.log(`level:(${level}) path:${entry.path}`)
    //console.log(`level of '${file_path}' is ${level}`)
    return level
}

function isFilenameSameAsParent(filePath) {
    const filename = basename(filePath, extname(filePath));
    const parentDirName = basename(dirname(filePath));
    return filename === parentDirName;
}

function get_url_type(file_path){
    if(file_path.toLowerCase().endsWith("readme.md")){
        return "dir"
    }else{
        if(isFilenameSameAsParent(file_path)){
            return "dir"
        }else{
            return "file"
        }
    }
}

async function createMarkdownDocumentSource(file_path){
    const url_type = get_url_type(file_path)
    const markdownText = await load_text(file_path)
    const {data} = matter(markdownText)
    const {entryFields, modelFields} = partitionFrontmatter(data ?? {})

    const slug = get_slug(entryFields,file_path,url_type)
    const url = entry_to_url(url_type,file_path,slug)
    const uid = buildDocumentUid(url, slug, file_path)
    const sid = shortMD5(uid)
    const level = entry_to_level(url_type,file_path)
    const base_dir = getDocumentBaseDir(file_path)
    const title = entryFields.title ?? slug
    const entry = {
        sid,
        uid,
        path:file_path,
        url,
        url_type,
        slug,
        title,
        level,
        base_dir
    }
    applyEntryOverrides(entry, entryFields)
    const modelAsset = createFrontmatterAsset(entry, modelFields)
    if(modelAsset){
        entry.model = modelAsset.uid
    }
    return {
        entry,
        markdownText:markdownText,
        modelAsset
    }
}

async function* collectMarkdownFileDocuments(){
    for await (const file_path of get_all_files(['md'])){
        const source = await createMarkdownDocumentSource(file_path)
        if(source){
            yield source
        }
    }
}

const MODEL_FILE_EXTENSIONS = new Set(['.yaml','.yml'])

async function* collectSingleFolderDocuments(){
    const buckets = new Map()
    for await (const file_path of get_all_files()){
        const extension = extname(file_path).toLowerCase()
        if(extension !== '.md' && !MODEL_FILE_EXTENSIONS.has(extension)){
            continue
        }
        const dir = dirname(file_path) || '.'
        if(!buckets.has(dir)){
            buckets.set(dir,{markdown:[],models:[]})
        }
        const bucket = buckets.get(dir)
        if(extension === '.md'){
            bucket.markdown.push(file_path)
        }else{
            bucket.models.push(file_path)
        }
    }
    const sortedDirs = Array.from(buckets.keys()).sort()
    for(const dir of sortedDirs){
        const bucket = buckets.get(dir)
        if(!bucket || bucket.markdown.length === 0){
            continue
        }
        bucket.markdown.sort()
        const sections = []
        for(const file_path of bucket.markdown){
            const raw = await load_text(file_path)
            const {content} = matter(raw)
            if(content && content.trim().length > 0){
                sections.push(content.trim())
            }
        }
        const markdownText = sections.join('\n\n')
        const primaryPath = bucket.markdown[0]
        const url_type = 'dir'
        const slug = get_slug({},primaryPath,url_type)
        const url = entry_to_url(url_type,primaryPath,slug)
        const uid = buildDocumentUid(url, slug, primaryPath)
        const sid = shortMD5(uid)
        const level = entry_to_level(url_type,primaryPath)
        const title = slug
        const base_dir = dir === '' ? '.' : dir
        const entry = {
            sid,
            uid,
            path:primaryPath,
            url,
            url_type,
            slug,
            title,
            level,
            base_dir,
            model:null
        }
        const modelAsset = createFolderModelAsset(entry,bucket.models)
        if(modelAsset){
            entry.model = modelAsset.uid
        }
        yield {
            entry,
            markdownText,
            modelAsset
        }
    }
}

function getDocumentBaseDir(file_path){
    const dir = dirname(file_path)
    if(dir === ''){
        return '.'
    }
    return dir
}

function createFrontmatterAsset(entry, frontmatter){
    const payload = frontmatter ?? {}
    const keys = Object.keys(payload)
    if(keys.length === 0){
        return null
    }
    const uid = `${entry.uid}#frontmatter`
    const asset = {
        type:"model",
        uid,
        sid:shortMD5(uid),
        document:entry.sid,
        parent_doc_uid:entry.uid,
        blob_content:JSON.stringify(payload)
    }
    return asset
}

function createFolderModelAsset(entry, modelFiles = []){
    if(!modelFiles || modelFiles.length === 0){
        return null
    }
    const sorted = [...modelFiles].sort()
    const selected = sorted[0]
    const uid = `${entry.uid}#${basename(selected)}`
    return {
        type:"model",
        uid,
        sid:shortMD5(uid),
        document:entry.sid,
        parent_doc_uid:entry.uid,
        path:selected
    }
}

async function* iterate_documents(){
    if(config.folder_single_doc){
        yield* collectSingleFolderDocuments()
    }else{
        yield* collectMarkdownFileDocuments()
    }
}

async function tree_content(markdown_text,entry_details){
    const {content, data} = matter(markdown_text)
    const tree = md_tree(content)
    const headings = extract_headings(tree,entry_details.uid)
    entry_details.headings = headings
    const tables = extract_tables(tree,headings,entry_details)
    entry_details.tables = tables
    const images = await extract_images(tree,headings,entry_details)
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

async function check_add_assets(asset_list,content_assets){
    const config = get_config()
    const referenced_locals = new Set()
    for(const asset of asset_list){
        if(Object.hasOwn(asset,"path")){
            referenced_locals.add(asset.path)
            let asset_exist = false
            let abs_path = ""
            if(asset.path.startsWith("/")){
                if(await exists_public(asset.path)){
                    asset_exist = true
                    abs_path = join(config.rootdir,"public",asset.path)
                }
            }else{
                if(await exists(asset.path)){
                    asset_exist = true
                    abs_path = join(config.contentdir,asset.path)
                }
            }
            //existence only relevant if really found anyway or for known extensions
            //otherwise it could be just a URL abs link
            if(asset_exist){
                asset.exists = asset_exist
                asset.abs_path = abs_path
            }else{
                if(asset.filter_ext){
                    asset.exists = asset_exist
                    warn(`(X) asset from filter ext does not exist '${asset.path}'`)
                }
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

function partitionFrontmatter(frontmatter = {}){
    const entryFields = {}
    const modelFields = {}
    for(const [key,value] of Object.entries(frontmatter)){
        if(KNOWN_ENTRY_FIELDS.has(key)){
            entryFields[key] = value
        }else{
            modelFields[key] = value
        }
    }
    return {entryFields, modelFields}
}

function applyEntryOverrides(entry, entryFields){
    for(const key of KNOWN_ENTRY_FIELDS){
        if((key === 'title' || key === 'slug') || !Object.hasOwn(entryFields,key)){
            continue
        }
        entry[key] = entryFields[key]
    }
}

function set_config(new_config){
    if(new_config != null){
        config = {
            ...config,
            ...new_config
        }
        if(config.folder_single_doc === undefined){
            config.folder_single_doc = false
        }
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
    check_add_assets,
    set_config,
    get_config,
    shortMD5,
    iterate_documents,
    tree_content
}
