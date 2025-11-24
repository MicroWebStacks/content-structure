import slugify from 'slugify'
import { readdir } from 'fs/promises'
import { file_ext, get_next_uid, load_text, exists } from './utils.js'
import {dirname, basename,parse, join} from 'path'
import {remark} from 'remark'
import remarkDirective from 'remark-directive'
import remarkGfm from 'remark-gfm';
import { JSDOM } from 'jsdom';
import { shortMD5, get_config } from './collect.js';
import { debug, warn } from './libs/log.js';
import yaml from 'js-yaml'

function safeDecodeURIComponent(value){
    try{
        return decodeURIComponent(value)
    }catch(_error){
        return value
    }
}

function sanitizeTag(value){
    if(value === null || value === undefined){
        return null
    }
    const trimmed = String(value).trim()
    if(!trimmed){
        return null
    }
    const decoded = safeDecodeURIComponent(trimmed)
    const normalized = decoded.replace(/\s+/g,' ')
    return slugify(normalized,{lower:true})
}

function ensureUniqueSlug(state, slug){
    const unique = get_next_uid(slug, state.assetSlugs)
    state.assetSlugs.push(unique)
    return unique
}

async function get_image_text(path){
    if(!await exists(path)){
        //silence warning as redundant with the assets check warning
        //warn(`(X) image ${path} does not exist`)
        return ""
    }
    if(!path.endsWith(".svg")){//only SVG supported for now
        return ""
    }
    const svgText = await load_text(path)

    const dom = new JSDOM(svgText, { contentType: 'image/svg+xml' });
    const textElements = dom.window.document.querySelectorAll('text');
    let result = [];
    textElements.forEach(textElement => {
        result.push(textElement.textContent)
    });
    debug(`   * found ${result.length} text entries in SVG`)
    return result
}

function node_text_list(node){
    const text_list = [];
    
    function traverse(node) {
        if((node.type == "text")||(node.type == "inlineCode")){
            text_list.push(node.value)
        }
        if(node.type == "textDirective"){
            const vars_val = Object.values(node.attributes).join(',')
            const directive_text = `${node.name}(${vars_val})`
            text_list.push(directive_text)
        }
        if (node.children) {
            for (const child of node.children) {
                traverse(child);
            }
        }
    }
    traverse(node)
    return text_list;
}

//also in (astro-big-doc)src\components\markdown\table\table.js
function astToDataTable(tableNode) {
    const data = [];
    for (const row of tableNode.children) {
        if (row.type === 'tableRow') {
        const rowData = [];
        for (const cell of row.children) {
            if (cell.type === 'tableCell') {
            const textNode = cell.children.find(child => child.type === 'text');
            if (textNode) {
                rowData.push(textNode.value);
            }
            }
        }

        data.push(rowData);
        }
    }

    return data;
}

function astToObjectsList(node){
    const [table_head, ...table_rows] = astToDataTable(node);
    const ObjectsList = table_rows.map(row => {
        let obj = {};
        table_head.forEach((header, index) => {
          obj[header] = row[index];
        });
        return obj;
      });
    return ObjectsList
}

function title_slug(title){
  const slug = slugify(title,{lower:true})
  return slug
}

function code_title_slug(node){
    if(!node?.meta){
        return null
    }
    const title = node.meta.split(/\s+/).join(' ')
    return sanitizeTag(title)
}

function image_name_slug(rawUrl){
    if(!rawUrl){
        return 'image'
    }
    const cleanPath = rawUrl.split(/[?#]/)[0]
    const filename = parse(basename(cleanPath)).name
    const slug = sanitizeTag(filename)
    return slug ?? 'image'
}

function link_slug(node,text){
    if(node.title !== null){
        return slugify(node.title,{lower:true})
    }
    return slugify(text)
}

function node_slug(node){
    let text_list = node_text_list(node);
    text_list = text_list.map((text)=>(text.trim()))
    const text_string = text_list.join('-')
    const slug = slugify(text_string,{lower:true})
    return slug
}

function node_text(node){
  let text_list = node_text_list(node);
  text_list = text_list.map((text)=>(text.trim()))
  return text_list.join(' ')
}

function md_tree(content) {
    const processor = remark()
        .use(remarkDirective)
        .use(remarkGfm)
    const markdownAST = processor.parse(content);
    return markdownAST;
}

function decodeAssetPath(pathValue){
    if(!pathValue){
        return pathValue
    }
    try{
        return decodeURIComponent(pathValue)
    }catch(error){
        warn(`(X) failed to decode asset path '${pathValue}': ${error.message}`)
        return pathValue
    }
}

function resolveDocumentAssetPath(entry,targetUrl){
    if(!targetUrl){
        return targetUrl
    }
    const baseDir = entry?.base_dir
    const documentDir = (baseDir && baseDir !== '') ? baseDir : dirname(entry?.path ?? '')
    const rawPath = targetUrl.startsWith("/")
        ? targetUrl
        : join(documentDir,targetUrl).replaceAll('\\','/')
    return decodeAssetPath(rawPath)
}

function isExternalAssetUrl(targetUrl){
    if(!targetUrl){
        return false
    }
    const trimmed = targetUrl.trim()
    if(trimmed.startsWith('//')){
        return true
    }
    return /^[a-zA-Z][a-zA-Z\d+\-.]*:/.test(trimmed)
}

async function buildDocumentContent(entry, markdownText){
    const tree = md_tree(markdownText ?? '')
    const state = await walkDocumentTree(tree, entry)
    return {
        tree,
        document:{
            headings:state.headings,
            tables:state.tables,
            images:state.images,
            code:state.codeBlocks,
            paragraphs:state.paragraphs,
            links:state.links
        },
        assets:state.assets
    }
}

async function walkDocumentTree(tree, entry){
    const config = get_config() ?? {}
    const allowedLinkExtensions = buildAllowedLinkExtensions(config.file_link_ext)
    const state = {
        entry,
        config,
        allowedLinkExtensions,
        headings:[],
        tables:[],
        images:[],
        codeBlocks:[],
        paragraphs:[],
        links:[],
        assets:[],
        assetSlugs:[],
        headingSlugs:[],
        linkSlugs:[],
        tableCounter:0,
        codeCounter:0,
        currentHeading:null,
        galleryAssetPaths:new Set()
    }

    async function visitNode(node){
        if(!node){
            return
        }
        switch(node.type){
            case 'heading':
                state.currentHeading = createHeadingEntry(node, state)
                break
            case 'table':
                createTableEntry(node, state)
                break
            case 'code':
                await createCodeEntry(node, state)
                break
            case 'paragraph':
                recordParagraph(node, state)
                break
            case 'image':
                await createImageEntry(node, state)
                break
            case 'link':
                await createLinkEntry(node, state)
                break
            default:
                break
        }
        if(Array.isArray(node.children)){
            for(const child of node.children){
                await visitNode(child)
            }
        }
    }

    if(Array.isArray(tree?.children)){
        for(const child of tree.children){
            await visitNode(child)
        }
    }
    return state
}

function buildAllowedLinkExtensions(values){
    if(!Array.isArray(values) || values.length === 0){
        return new Set()
    }
    const normalized = values
        .map(ext => typeof ext === 'string' ? ext.toLowerCase() : '')
        .filter(Boolean)
    return new Set(normalized)
}

function getHeadingSlug(state){
    return state?.currentHeading?.slug ?? null
}

function createHeadingEntry(node, state){
    const heading_text = node_text(node)
    const heading_slug = title_slug(heading_text)
    const unique_heading_slug = get_next_uid(heading_slug,state.headingSlugs)
    state.headingSlugs.push(unique_heading_slug)
    const uid = `${state.entry.uid}#${unique_heading_slug}`
    const entry = {
        label:heading_text,
        slug:unique_heading_slug,
        uid,
        sid:shortMD5(uid),
        depth:node.depth,
        line:node.position?.start?.line ?? null
    }
    state.headings.push(entry)
    return entry
}

function createTableEntry(node, state){
    state.tableCounter += 1
    const id = `table-${state.tableCounter}`
    const slug = ensureUniqueSlug(state, id)
    const uid = `${state.entry.uid}#${slug}`
    const data = astToObjectsList(node)
    const tableEntry = {
        id,
        uid,
        sid:shortMD5(uid),
        heading:getHeadingSlug(state),
        text:node_text(node),
        data
    }
    state.tables.push(tableEntry)
    state.assets.push({
        type:'table',
        uid:tableEntry.uid,
        sid:tableEntry.sid,
        document:state.entry.sid,
        parent_doc_uid:state.entry.uid,
        blob_content:JSON.stringify(data ?? [])
    })
}

async function createCodeEntry(node, state){
    const language = node.lang ? node.lang : null
    const languageTag = language ? sanitizeTag(language) : null
    const metaRaw = typeof node.meta === 'string' ? node.meta : null
    const metaSlug = metaRaw ? sanitizeTag(metaRaw) : null
    const isGallery = (languageTag === 'yaml') && (typeof metaRaw === 'string' && metaRaw.trim().toLowerCase() === 'gallery')
    state.codeCounter += 1
    const titleSlug = code_title_slug(node)
    const baseName = titleSlug ? `code-${state.codeCounter}-${titleSlug}` : `code-${state.codeCounter}`
    const metaAwareBase = metaSlug ? `code-${state.codeCounter}-${metaSlug}` : baseName
    const slugBase = languageTag ? `${metaAwareBase}.${languageTag}` : metaAwareBase
    const slug = ensureUniqueSlug(state, slugBase)
    const uid = `${state.entry.uid}#${slug}`
    const normalizedLanguage = languageTag ?? (typeof language === 'string' ? language.trim().toLowerCase() : null)
    const codeEntry = {
        id:baseName,
        uid,
        sid:shortMD5(uid),
        language,
        heading:getHeadingSlug(state),
        text:node.value,
        meta:metaRaw
    }
    state.codeBlocks.push(codeEntry)
    state.assets.push({
        type:'codeblock',
        uid:codeEntry.uid,
        sid:codeEntry.sid,
        document:state.entry.sid,
        parent_doc_uid:state.entry.uid,
        blob_content:codeEntry.text ?? '',
        language:codeEntry.language,
        ext:normalizedLanguage ?? null,
        meta:metaRaw
    })
    if(isGallery){
        await collectGalleryAssets(node, state, codeEntry)
    }
}

function recordParagraph(node, state){
    const text = node_text(node)
    if(!text || !text.trim()){
        return
    }
    state.paragraphs.push({
        heading:getHeadingSlug(state),
        text
    })
}

async function createImageEntry(node, state){
    const rawUrl = typeof node.url === 'string' ? node.url.trim() : ''
    const extRaw = file_ext(rawUrl)
    const extTag = sanitizeTag(extRaw)
    const baseName = image_name_slug(rawUrl)
    const slugBase = extTag ? `image-${baseName}.${extTag}` : `image-${baseName}`
    const slug = ensureUniqueSlug(state, slugBase)
    const uid = `${state.entry.uid}#${slug}`
    const imageEntry = {
        id:baseName,
        uid,
        sid:shortMD5(uid),
        heading:getHeadingSlug(state),
        title:node.title,
        url:node.url,
        alt:node.alt,
        text_list:[]
    }
    const asset = await buildImageAsset(node, state, imageEntry, extRaw)
    if(asset){
        state.assets.push(asset)
    }
    state.images.push(imageEntry)
}

async function buildImageAsset(node, state, imageEntry, extRaw){
    const rawUrl = typeof node.url === 'string' ? node.url.trim() : ''
    if(!rawUrl || isExternalAssetUrl(rawUrl)){
        return null
    }
    const path = resolveDocumentAssetPath(state.entry, rawUrl)
    if(!path || path.startsWith('/')){
        return null
    }
    const existsLocally = await exists(path)
    if(!existsLocally){
        return null
    }
    const textList = await get_image_text(path)
    if(textList !== undefined && textList !== null){
        imageEntry.text_list = textList
    }
    const asset = {
        type:'image',
        uid:imageEntry.uid,
        sid:imageEntry.sid,
        document:state.entry.sid,
        parent_doc_uid:state.entry.uid,
        path,
        ext:extRaw ?? file_ext(rawUrl),
        exists:true,
        abs_path:join(state.config.contentdir ?? '', path)
    }
    return asset
}

async function createLinkEntry(node, state){
    const text = node_text(node)
    const slug = link_slug(node,text)
    const unique_slug = get_next_uid(slug,state.linkSlugs)
    state.linkSlugs.push(unique_slug)
    const linkEntry = {
        id:unique_slug,
        heading:getHeadingSlug(state),
        url:node.url,
        title:node.title,
        text
    }
    state.links.push(linkEntry)
    if(state.allowedLinkExtensions.size === 0){
        return
    }
    const asset = await buildLinkAsset(node, state, linkEntry)
    if(asset){
        state.assets.push(asset)
    }
}

async function buildLinkAsset(node, state, linkEntry){
    const rawUrl = typeof node.url === 'string' ? node.url.trim() : ''
    if(!rawUrl || isExternalAssetUrl(rawUrl) || rawUrl.startsWith('/')){
        return null
    }
    const extension = file_ext(rawUrl).toLowerCase()
    if(!state.allowedLinkExtensions.has(extension)){
        return null
    }
    const path = resolveDocumentAssetPath(state.entry, rawUrl)
    if(!path || path.startsWith('/')){
        return null
    }
    const existsLocally = await exists(path)
    if(!existsLocally){
        return null
    }
    const assetId = linkEntry.id ? `link-${linkEntry.id}` : `link-${shortMD5(rawUrl)}`
    const uid = `${state.entry.uid}#${assetId}`
    return {
        type:'linked_file',
        uid,
        sid:shortMD5(uid),
        document:state.entry.sid,
        parent_doc_uid:state.entry.uid,
        path,
        ext:extension,
        exists:true,
        abs_path:join(state.config.contentdir ?? '', path)
    }
}

function normalizeRelativeAssetPath(pathValue){
    if(typeof pathValue !== 'string'){
        return null
    }
    const trimmed = pathValue.trim()
    if(!trimmed){
        return null
    }
    if(trimmed.startsWith('/')){
        return null
    }
    return trimmed.replace(/^[.][\\/]/,'').replaceAll('\\','/')
}

async function collectGalleryAssets(node, state, codeEntry){
    const parsed = parseGalleryYaml(node?.value)
    if(parsed === null){
        return
    }
    const paths = []
    if(Array.isArray(parsed)){
        for(const entry of parsed){
            if(typeof entry === 'string' && entry.trim()){
                paths.push(entry)
            }else{
                warn(`(X) skipping non-string gallery entry`)
            }
        }
    }else if(parsed && typeof parsed === 'object' && !Array.isArray(parsed)){
        const keys = Object.keys(parsed)
        if(keys.length === 1 && keys[0] === 'dir' && typeof parsed.dir === 'string' && parsed.dir.trim()){
            const dirRaw = parsed.dir.trim()
            const dirPath = resolveDocumentAssetPath(state.entry, dirRaw)
            if(!dirPath || dirPath.startsWith('/')){
                warn(`(X) gallery dir is invalid or absolute '${parsed.dir}'`)
                return
            }
            const files = await listGalleryDirFiles(dirPath, state)
            for(const fileName of files){
                paths.push(join(dirRaw, fileName).replaceAll('\\','/'))
            }
        }else{
            warn(`(X) gallery yaml must be a list or {dir: <path>}`)
            return
        }
    }else{
        warn(`(X) gallery yaml must be a list or {dir: <path>}`)
        return
    }
    for(const rawPath of paths){
        await addGalleryAsset(rawPath, state, codeEntry)
    }
}

function parseGalleryYaml(raw){
    if(typeof raw !== 'string' || !raw.trim()){
        return null
    }
    try{
        return yaml.load(raw)
    }catch(error){
        warn(`(X) failed to parse gallery yaml: ${error.message}`)
        return null
    }
}

async function listGalleryDirFiles(dirPath, state){
    const absDir = join(state.config.contentdir ?? '', dirPath)
    try{
        const entries = await readdir(absDir,{withFileTypes:true})
        return entries.filter(entry => entry.isFile()).map(entry => entry.name)
    }catch(error){
        warn(`(X) failed to read gallery dir '${dirPath}': ${error.message}`)
        return []
    }
}

async function addGalleryAsset(rawPath, state, codeEntry){
    const normalized = normalizeRelativeAssetPath(rawPath)
    if(!normalized){
        return
    }
    const resolvedPath = resolveDocumentAssetPath(state.entry, normalized)
    if(!resolvedPath || resolvedPath.startsWith('/')){
        return
    }
    const cleanedPath = resolvedPath.replace(/^[.][\\/]/,'').replaceAll('\\','/')
    if(state.galleryAssetPaths.has(cleanedPath)){
        return
    }
    const existsLocally = await exists(cleanedPath)
    if(!existsLocally){
        warn(`(X) gallery asset does not exist '${cleanedPath}'`)
        return
    }
    state.galleryAssetPaths.add(cleanedPath)
    const baseName = image_name_slug(cleanedPath)
    const slugBase = `${codeEntry.id}.${baseName}`
    const slug = ensureUniqueSlug(state, slugBase)
    const uid = `${state.entry.uid}#${slug}`
    state.assets.push({
        type:'gallery_asset',
        uid,
        sid:shortMD5(uid),
        document:state.entry.sid,
        parent_doc_uid:state.entry.uid,
        path:cleanedPath,
        ext:file_ext(cleanedPath),
        exists:true,
        abs_path:join(state.config.contentdir ?? '', cleanedPath)
    })
}

export{
    md_tree,
    buildDocumentContent,
    node_text_list,
    node_slug,
    title_slug,
    node_text
}
