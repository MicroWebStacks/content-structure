import slugify from 'slugify'
import { file_ext, get_next_uid, load_text, exists } from './utils.js'
import {dirname, basename,parse, extname, join} from 'path'
import {remark} from 'remark'
import remarkDirective from 'remark-directive'
import remarkGfm from 'remark-gfm';
import { JSDOM } from 'jsdom';
import { shortMD5, get_config } from './collect.js';
import { debug, warn } from './libs/log.js';

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

function code_slug(node,language){
    let slug = language
    if(node.meta != null){
        slug += '-'+node.meta.split(/\s+/).join('-')
    }
    return slug
}

function image_slug(node){
    if(node.title !== null){
        return slugify(node.title,{lower:true})
    }
    if(node.alt !== null){
        return slugify(node.alt,{lower:true})
    }
    const filename = parse(basename(node.url)).name
    return slugify(filename,{lower:true})
    
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
        headingSlugs:[],
        imageSlugs:[],
        codeSlugs:[],
        linkSlugs:[],
        tableCounter:0,
        currentHeading:null
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
                createCodeEntry(node, state)
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
    const uid = `${state.entry.uid}#${id}`
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

function createCodeEntry(node, state){
    const language = node.lang ? node.lang : 'code'
    const slug = code_slug(node,language)
    const unique_slug = get_next_uid(slug,state.codeSlugs)
    state.codeSlugs.push(unique_slug)
    const uid = `${state.entry.uid}#${unique_slug}`
    const codeEntry = {
        id:unique_slug,
        uid,
        sid:shortMD5(uid),
        language,
        heading:getHeadingSlug(state),
        text:node.value
    }
    state.codeBlocks.push(codeEntry)
    state.assets.push({
        type:'codeblock',
        uid:codeEntry.uid,
        sid:codeEntry.sid,
        document:state.entry.sid,
        parent_doc_uid:state.entry.uid,
        blob_content:codeEntry.text ?? '',
        language:codeEntry.language
    })
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
    const slug = image_slug(node)
    const unique_slug = get_next_uid(slug,state.imageSlugs)
    state.imageSlugs.push(unique_slug)
    const uid = `${state.entry.uid}#${unique_slug}`
    const imageEntry = {
        id:unique_slug,
        uid,
        sid:shortMD5(uid),
        heading:getHeadingSlug(state),
        title:node.title,
        url:node.url,
        alt:node.alt,
        text_list:[]
    }
    const asset = await buildImageAsset(node, state, imageEntry)
    if(asset){
        state.assets.push(asset)
    }
    state.images.push(imageEntry)
}

async function buildImageAsset(node, state, imageEntry){
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
        ext:file_ext(rawUrl),
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

export{
    md_tree,
    buildDocumentContent,
    node_text_list,
    node_slug,
    title_slug,
    node_text
}
