import slugify from 'slugify'
import { get_next_uid,load_text } from './utils.js'
import {visit} from "unist-util-visit";
import {dirname, basename,parse, extname} from 'path'
import {remark} from 'remark'
import remarkDirective from 'remark-directive'
import remarkGfm from 'remark-gfm';
import {remarkTags} from './ast-tags.js'
import {join} from 'path'
import { exists } from './utils.js';
import { JSDOM } from 'jsdom';
import { get_config,shortMD5 } from './collect.js';

async function get_image_text(path){
    if(!await exists(path)){
        console.warn(`   (X) file ${path} does not exist`)
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
    console.log(`   * found ${result.length} text entries in SVG`)
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

    const new_markdownAST = remarkTags(markdownAST,get_config().tags)
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
                text:heading_text,
                slug:unique_heading_slug,
                depth:node.depth,
                line:node.position.start.line
            })
        }
    })
    return headings_list
}

function extract_tables(tree,headings){
    let tables_list = []
    let id = 1;
    visit(tree, node=> {
        if (node.type === 'table') {
            tables_list.push({
                id:`table-${id}`,
                heading:heading_from_line(headings,node.position.start.line),
                cells:node_text_list(node),
            })
            id+=1
        }
    })
    return tables_list
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

function extract_code(tree,headings){
    let code_list = []
    let code_slug_list = []
    visit(tree, node=> {
        if (node.type === 'code') {
            const slug = code_slug(node)
            const unique_slug = get_next_uid(slug,code_slug_list)
            code_slug_list.push(unique_slug)
            code_list.push({
                id:unique_slug,
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
            const uid = `${entry.uid}#${code.id}`
            codes.push({
                type:"code",
                uid:uid,
                sid:shortMD5(uid),
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

function extract_tags(tree,headings){
    let tags_list = []
    visit(tree, 'tag',node=> {
        tags_list.push({
            heading:heading_from_line(headings,node.position.start.line),
            type:node.tag_type,
            value:node.tag_value
        })
    })
    return tags_list
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
    extract_tags,
    get_images_info,
    get_codes_info,
    get_links_info
}
