import {existsSync,copyFileSync,mkdirSync,statSync} from 'fs'
import {promises as fs} from 'fs';
import {resolve,dirname,join,relative} from 'path'
import {get_config} from './collect.js'


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
  try {
      await fs.access(abs_dir)
  } catch {
    if(config.debug){
      console.log(`mkdir : '${abs_dir}'`)
    }
    await fs.mkdir(abs_dir, { recursive: true });
  }
}

async function save_json(data,file_path){
  const config = get_config()
  const filepath = join(config.rootdir,config.rel_outdir,file_path)
  await fs.writeFile(filepath,JSON.stringify(data,undefined, 2))
  if(config.debug){
    console.log(` saved json file ${filepath}`)
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

export{
    relAssetToUrl,
    check_dir_create,
    save_json,
    get_next_uid
}
