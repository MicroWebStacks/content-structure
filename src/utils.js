import {existsSync,copyFileSync,mkdirSync,statSync} from 'fs'
import { readFile, writeFile, access, mkdir } from 'fs/promises';
import { constants as fs_constants } from 'fs/promises';
import {resolve,dirname,join,relative} from 'path'
import {get_config} from './collect.js'
import yaml from 'js-yaml';


function isNewer(filepath,targetfile){
  const t1 = statSync(filepath).mtime
  const t2 = statSync(targetfile).mtime
  return (t1>t2)
}

async function check_dir_create(dirname){
  const config = get_config()
  const abs_dir = join(config.outdir,dirname)
  if(!await exists_abs(abs_dir)){
    if(config.debug){
      console.log(`mkdir : '${abs_dir}'`)
    }
    await mkdir(abs_dir, { recursive: true });
  }
}

function get_next_uid(url,uid_list){
  let counter = 1;
  let newUrl = url;
  
  while (uid_list.includes(newUrl)) {
      counter++;
      newUrl = `${url}-${counter}`;
  }

  return newUrl;
}

async function exists_abs(abs_path) {
  abs_path = decodeURIComponent(abs_path)
  try {
    await access(abs_path, fs_constants.F_OK);
    return true;
  } catch (error) {
    return false;
  }
}

async function exists(rel_path) {
  const config = get_config()
  const path = join(config.contentdir,rel_path)
  return await exists_abs(path)
}

async function exists_public(rel_path) {
  const config = get_config()
  const path = join(config.rootdir,"public",rel_path)
  return await exists_abs(path)
}

// => out dir
async function save_json(data,file_path){
  const config = get_config()
  const filepath = join(config.outdir,file_path)
  await writeFile(filepath,JSON.stringify(data,undefined, 2))
  //if(config.debug){
  //  console.log(` saved json file ${filepath}`)
  //}
}

// content dir =>
async function load_yaml(rel_path){
  const config = get_config()
  const path = join(config.contentdir,rel_path)
  const fileContent = await readFile(path, 'utf8');
  const data = yaml.load(fileContent);
  return data;
}

async function load_yaml_code(rel_path){
  let currentPath = decodeURIComponent(new URL(import.meta.url).pathname)
  // Remove leading slash on Windows (e.g., /D:/... becomes D:/...)
  if (process.platform === 'win32' && currentPath.match(/^\/[a-zA-Z]:\//)) {
    currentPath = currentPath.substring(1)
  }
  const currentDir = dirname(currentPath)
  const parentRoot = dirname(currentDir)
  const path = join(parentRoot, rel_path)
  const fileContent = await readFile(path,'utf8')
  return yaml.load(fileContent)
}

// content dir =>
async function load_json(rel_path,dir="content"){
  const config = get_config()
  const abs_folder = (dir=="content")?config.contentdir:config.outdir
  const path = join(abs_folder,rel_path)
  const text = await readFile(path,'utf-8')
  return JSON.parse(text)
}

async function load_text(rel_path){
  const config = get_config()
  const path = join(config.contentdir,rel_path)
  const filepath = decodeURIComponent(path)//could be an image url
  const text = await readFile(filepath,'utf-8')
  return text
}

function list_to_map(input_list,field){
  const output_map = input_list.reduce((acc, obj) => {
    acc[obj[field]] = obj;
    return acc;
  }, {});
  return output_map
}

function add_documents(asset_map,documents){
  const output_map = documents.reduce((acc, obj) => {
    acc[obj.sid] = {
      type:"document",
      uid:obj.uid
    };
    return acc;
  }, asset_map);
  return output_map
}

function file_ext(url){
  url = url.split('?')[0].split('#')[0];
  const filename = url.substring(url.lastIndexOf('/') + 1);
  const lastDotIndex = filename.lastIndexOf('.');
  return (lastDotIndex === -1) ? '' : filename.substring(lastDotIndex + 1)
}

export{
    check_dir_create,
    save_json,
    get_next_uid,
    exists,
    exists_public,
    exists_abs,
    load_yaml,
    load_yaml_code,
    load_json,
    load_text,
    list_to_map,
    add_documents,
    file_ext
}
