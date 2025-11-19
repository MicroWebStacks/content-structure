import slugify from 'slugify'
import { file_ext, get_next_uid,load_text } from './utils.js'
import {visit} from "unist-util-visit";
import {dirname, basename,parse, extname} from 'path'
import {remark} from 'remark'
import remarkDirective from 'remark-directive'
import remarkGfm from 'remark-gfm';
import {join} from 'path'
import { exists } from './utils.js';
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

function heading_from_line(headings,line){
    for(let i=headings.length-1;i>=0;i--){
        if(headings[i].line < line){
            return headings[i].slug
        }
    }
    return null
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

function extract_headings(tree,doc_uid){
    let headings_list = []
    let heading_slug_list = []
    visit(tree, node=> {
        if (node.type === 'heading') {
            const heading_text = node_text(node)
            const heading_slug = title_slug(heading_text)
            const unique_heading_slug = get_next_uid(heading_slug,heading_slug_list)
            heading_slug_list.push(unique_heading_slug)
            const uid = doc_uid+"#"+unique_heading_slug
            headings_list.push({
                label:heading_text,
                slug:unique_heading_slug,
                uid: uid,
                sid:shortMD5(uid),
                depth:node.depth,
                line:node.position.start.line
            })
        }
    })
    return headings_list
}

function extract_tables(tree,headings,entry){
    let tables_list = []
    let count = 1;
    visit(tree, node=> {
        if (node.type === 'table') {
            const id = `table-${count}`
            const uid = `${entry.uid}#${id}`
            const data = astToObjectsList(node)
            tables_list.push({
                id:id,
                uid:uid,
                sid:shortMD5(uid),
                heading:heading_from_line(headings,node.position.start.line),
                text:node_text(node),
                data:data
            })
            count+=1
        }
    })
    return tables_list
}

function get_tables_info(entry,content){
    const tables = []
    if(content.tables.length > 0){
        for(const table of content.tables){
            const serialized = JSON.stringify(table.data ?? [])
            tables.push({
                type:"table",
                uid:table.uid,
                sid:table.sid,
                document:entry.sid,
                parent_doc_uid:entry.uid,
                blob_content:serialized
            })
        }
    }
    return tables
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

async function extract_images(tree, headings,entry) {
    const fileDir = dirname(entry.path)
    let images_slug_list = [];
    let imagePromises = [];
    async function processImage(node) {
            const slug = image_slug(node);
            const unique_slug = get_next_uid(slug, images_slug_list);
            images_slug_list.push(unique_slug);
            const image_text = await get_image_text(join(fileDir,node.url));
            const uid = `${entry.uid}#${unique_slug}`
            return {
                id: unique_slug,
                uid:uid,
                sid:shortMD5(uid),
                heading: heading_from_line(headings, node.position.start.line),
                title: node.title,
                url: node.url,
                alt: node.alt,
                text_list: image_text,
            }
   }
   visit(tree, 'image', (node) => {imagePromises.push(processImage(node))});
   const images_list = await Promise.all(imagePromises);
   return images_list;
}

async function get_images_info(entry,content){
    const file_links = []
    if((content.images ?? []).length > 0){
        for(const image of content.images){
            if(isExternalAssetUrl(image.url)){
                continue
            }
            const path = resolveDocumentAssetPath(entry,image.url)
            if(!path || path.startsWith('/')){
                continue
            }
            const existsLocally = await exists(path)
            if(!existsLocally){
                continue
            }
            file_links.push({
                type:"image",
                uid:image.uid,
                sid:image.sid,
                document:entry.sid,
                parent_doc_uid:entry.uid,
                path:path,
                ext:file_ext(image.url)
            })
        }
    }
    return file_links
}

function extract_code(tree,headings,entry){
    let code_list = []
    let code_slug_list = []
    visit(tree, node=> {
        if (node.type === 'code') {
            const language = node.lang?node.lang:"code"
            const slug = code_slug(node,language)
            const unique_slug = get_next_uid(slug,code_slug_list)
            const uid = `${entry.uid}#${unique_slug}`
            code_slug_list.push(unique_slug)
            code_list.push({
                id:unique_slug,
                uid:uid,
                sid:shortMD5(uid),
                language:language,
                heading:heading_from_line(headings,node.position.start.line),
                text:node.value
            })
        }
    })
    return code_list
}

function get_codes_info(entry,content){
    const codes = []    
    if(content.code.length > 0){
        for(const code of content.code){
            codes.push({
                type:"codeblock",
                uid:code.uid,
                sid:code.sid,
                document:entry.sid,
                parent_doc_uid:entry.uid,
                blob_content:code.text ?? '',
                language:code.language
            })
        }
    }
    return codes
}

//here we get paragraphs text only for search and returning sections 
//but without images, tables, code content (inlineCode stays as text)
function extract_paragraphs(tree,headings){
    let paragraphs_list = []
    visit(tree, "paragraph", node=> {
        paragraphs_list.push({
            heading:heading_from_line(headings,node.position.start.line),
            text:node_text(node)
        })
    })
    return paragraphs_list
}

function extract_links(tree,headings){
    let links_list = []
    let slug_list = [];
    visit(tree, "link", node=> {
        const text = node_text(node)
        const slug = link_slug(node,text)
        const unique_slug = get_next_uid(slug, slug_list);
        links_list.push({
            id:unique_slug,
            heading:heading_from_line(headings,node.position.start.line),
            url: node.url,
            title: node.title,
            text: text
        })
    })
    return links_list
}

async function get_links_info(entry, content){
    const links = content?.links ?? []
    if(links.length === 0){
        return []
    }
    const {file_link_ext = []} = get_config() ?? {}
    if(!Array.isArray(file_link_ext) || file_link_ext.length === 0){
        return []
    }
    const allowedExtensions = new Set(file_link_ext.map(ext => typeof ext === 'string' ? ext.toLowerCase() : '').filter(Boolean))
    if(allowedExtensions.size === 0){
        return []
    }
    const assets = []
    for(const link of links){
        const rawUrl = typeof link.url === 'string' ? link.url.trim() : ''
        if(!rawUrl){
            continue
        }
        if(isExternalAssetUrl(rawUrl)){
            continue
        }
        if(rawUrl.startsWith('/')){
            continue
        }
        const extension = file_ext(rawUrl).toLowerCase()
        if(!allowedExtensions.has(extension)){
            continue
        }
        const path = resolveDocumentAssetPath(entry,rawUrl)
        if(!path || path.startsWith('/')){
            continue
        }
        const existsLocally = await exists(path)
        if(!existsLocally){
            continue
        }
        const assetId = link.id ? `link-${link.id}` : `link-${shortMD5(rawUrl)}`
        const uid = `${entry.uid}#${assetId}`
        assets.push({
            type:"linked_file",
            uid,
            sid:shortMD5(uid),
            document:entry.sid,
            parent_doc_uid:entry.uid,
            path:path,
            ext:extension
        })
    }
    return assets
}

export{
    md_tree,
    extract_headings,
    extract_tables,
    extract_images,
    extract_code,
    extract_paragraphs,
    extract_links,
    get_links_info,
    node_text_list,
    node_slug,
    title_slug,
    node_text,
    get_images_info,
    get_tables_info,
    get_codes_info
}
