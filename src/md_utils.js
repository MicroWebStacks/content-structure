import slugify from 'slugify'
import { get_next_uid,load_text } from './utils.js'
import {visit} from "unist-util-visit";
import {dirname, basename,parse, extname} from 'path'
import {remark} from 'remark'
import remarkDirective from 'remark-directive'
import remarkGfm from 'remark-gfm';
import {remarkMatches} from './node-text-matches.js'
import {join} from 'path'
import { exists } from './utils.js';
import { JSDOM } from 'jsdom';
import { get_config,shortMD5 } from './collect.js';
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

function code_slug(node){
    let slug = node.lang
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
    const new_markdownAST = remarkMatches(markdownAST,get_config().matches)
    return new_markdownAST;
}

function extract_headings(tree){
    let headings_list = []
    let heading_slug_list = []
    visit(tree, node=> {
        if (node.type === 'heading') {
            const heading_text = node_text(node)
            const heading_slug = title_slug(heading_text)
            const unique_heading_slug = get_next_uid(heading_slug,heading_slug_list)
            heading_slug_list.push(unique_heading_slug)
            headings_list.push({
                label:heading_text,
                slug:unique_heading_slug,
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
            const text = node_text_list(node).join(" ")
            const data = astToObjectsList(node)
            tables_list.push({
                id:id,
                uid:uid,
                sid:shortMD5(uid),
                heading:heading_from_line(headings,node.position.start.line),
                text:text,
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
            tables.push({
                type:"table",
                uid:table.uid,
                sid:table.sid,
                document:entry.sid
            })
        }
    }
    return tables
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
                text: image_text,
            }
   }
   visit(tree, 'image', (node) => {imagePromises.push(processImage(node))});
   const images_list = await Promise.all(imagePromises);
   return images_list;
}

function get_images_info(entry,content){
    const images = []
    if(content.images.length > 0){
        for(const image of content.images){
            const path = join(dirname(entry.path),image.url).replaceAll('\\','/')
            images.push({
                type:"image",
                uid:image.uid,
                sid:image.sid,
                document:entry.sid,
                path:path
            })
        }
    }
    return images
}

function extract_code(tree,headings,entry){
    let code_list = []
    let code_slug_list = []
    visit(tree, node=> {
        if (node.type === 'code') {
            const slug = code_slug(node)
            const unique_slug = get_next_uid(slug,code_slug_list)
            const uid = `${entry.uid}#${unique_slug}`
            code_slug_list.push(unique_slug)
            code_list.push({
                id:unique_slug,
                uid:uid,
                sid:shortMD5(uid),
                language:node.lang?node.lang:"code",
                heading:heading_from_line(headings,node.position.start.line),
                value:node.value
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
                type:"code",
                uid:code.uid,
                sid:code.sid,
                hash:shortMD5(code.value),
                document:entry.sid,
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
            text:node_text_list(node)
        })
    })
    return paragraphs_list
}

function extract_links(tree,headings){
    let links_list = []
    let slug_list = [];
    visit(tree, "link", node=> {
        const text = node_text_list(node).join(" ")
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

function get_links_info(entry,content,assets_ext){
    const links = []    
    if(content.links.length > 0){
        for(const link of content.links){
            const external = link.url.startsWith('http')
            const uid = `${entry.uid}#${link.id}`
            let newlink = {
                type:"link",
                uid:uid,
                sid:shortMD5(uid),
                text:link.text,
                document:entry.sid,
                external:external
            }
            if(external){
                newlink.url = link.url
            }else{
                const path = join(dirname(entry.path),link.url).replaceAll('\\','/')
                const ext = extname(path).slice(1)
                if(assets_ext.includes(ext)){
                    newlink.path = path
                }
                else{
                    newlink.url = path
                }
            }
            links.push(newlink)
        }
    }
    return links
}

function extract_refs(tree,headings){
    let refs_list = []
    visit(tree, 'reference',node=> {
        refs_list.push({
            heading:heading_from_line(headings,node.position.start.line),
            type:node.ref_type,
            value:node.ref_value
        })
    })
    return refs_list
}

function get_refs_info(entry,all_items_map){
    const references = entry.references.map((ref)=>({
        source_type:"document",
        source_sid:entry.sid,
        ...ref
    }))
    for(const image of entry.images){
        const image_entries = image.references.map((ref)=>({
            source_type:"image",
            source_sid:image.sid,
            heading:image.heading,
            ...ref
        }))
        references.push(...image_entries)
    }
    const refs = []
    for(const ref of references){
        const target_sid = (ref.type=="page")?shortMD5(ref.value):ref.value
        if(!Object.hasOwn(all_items_map,target_sid)){
            warn(`(X) dropping reference '${ref.value}' that does not exist, referenced from '${ref.source_sid}'`)
            continue
        }
        const target_Asset = all_items_map[target_sid]
        refs.push({
            source_type:ref.source_type,
            source_sid:ref.source_sid,
            source_heading:ref.heading,
            target_type:target_Asset.type,
            target_uid:target_Asset.uid,
            target_sid:target_sid
        })
    }
    return refs
}

export{
    md_tree,
    extract_headings,
    extract_tables,
    extract_images,
    extract_code,
    extract_paragraphs,
    extract_links,
    node_text_list,
    node_slug,
    title_slug,
    node_text,
    extract_refs,
    get_images_info,
    get_tables_info,
    get_codes_info,
    get_links_info,
    get_refs_info
}
