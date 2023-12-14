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

//Note 'imp*ort.me*ta.en*v.BA*SE_URL' only works from Astro component not from remark-rel-asset plugin
function relAssetToUrl(relativepath,refFile){
  const refdir = join("content",dirname(refFile))
    let newurl = relativepath
    const filepath = join(refdir,relativepath)
    console.log(`relAssetToUrl> filepath = ${filepath}`)
    if(existsSync(filepath)){
      //console.log(`   * impo*rt.me*ta.ur*l = ${import.meta.url}`)
      const config = get_config()
      let rel_outdir = config.rel_outdir
      if(import.meta.env.MODE == "development"){
        rel_outdir = "public"
      }
      const targetroot = join(config.rootdir,rel_outdir,"raw")
      const filerootrel = relative(config.rootdir,refdir)
      const targetpath = resolve(targetroot,filerootrel)
      const targetfile = join(targetpath,relativepath)
      const targetdir = dirname(targetfile)
      //console.log(`copy from '${filepath}' to '${targetfile}'`)
      const newpath = join("raw/",filerootrel,relativepath)
      newurl = newpath.replaceAll('\\','/')
      if(!existsSync(targetdir)){
        mkdirSync(targetdir,{ recursive: true })
      }
      if(!existsSync(targetfile)){
        copyFileSync(filepath,targetfile)
        console.log(`utils.js> * new asset url = '${newurl}'`)
      }
      else if(isNewer(filepath,targetfile)){
        copyFileSync(filepath,targetfile)
        console.log(`utils.js> * updated asset url = '${newurl}'`)
      }else{
        console.log(`utils.js> * existing asset url = '${newurl}'`)
      }
    }

    return newurl
}

async function check_dir_create(dirname){
  const config = get_config()
  const abs_dir = join(config.rootdir,config.rel_outdir,dirname)
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
  try {
    await access(abs_path, fs_constants.F_OK);
    return true;
  } catch (error) {
    return false;
  }
}

async function exists(rel_path) {
  const config = get_config()
  const path = join(config.rootdir,config.rel_contentdir,rel_path)
  return await exists_abs(path)
}


// => out dir
async function save_json(data,file_path){
  const config = get_config()
  const filepath = join(config.rootdir,config.rel_outdir,file_path)
  await writeFile(filepath,JSON.stringify(data,undefined, 2))
  //if(config.debug){
  //  console.log(` saved json file ${filepath}`)
  //}
}

// content dir =>
async function load_yaml(rel_path){
  const config = get_config()
  const path = join(config.rootdir,config.rel_contentdir,rel_path)
  const fileContent = await readFile(path, 'utf8');
  const data = yaml.load(fileContent);
  return data;
}

// content dir =>
async function load_json(rel_path,dir="content"){
  const config = get_config()
  const rel_folder = (dir=="content")?config.rel_contentdir:config.rel_outdir
  const path = join(config.rootdir,rel_folder,rel_path)
  const text = await readFile(path,'utf-8')
  return JSON.parse(text)
}

async function load_text(rel_path){
  const config = get_config()
  const path = join(config.rootdir,config.rel_contentdir,rel_path)
  const text = await readFile(path,'utf-8')
  return text
}

export{
    relAssetToUrl,
    check_dir_create,
    save_json,
    get_next_uid,
    exists,
    exists_abs,
    load_yaml,
    load_json,
    load_text
}
