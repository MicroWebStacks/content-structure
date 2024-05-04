import {collect, getDocuments, getEntry} from 'content-structure'
import {fileURLToPath} from 'url';
import {dirname,join} from 'path'

const rootdir = dirname(fileURLToPath(import.meta.url))

await collect({
    rootdir:rootdir,
    contentdir:join(rootdir,"content"),
    content_ext:["md","json","yml","yaml"],
    assets_ext:["svg","webp","png","jpeg","jpg","xlsx","glb"],
    outdir:join(rootdir,".structure"),
    debug:true
})

const documents = await getDocuments()
console.log(`\nobtained ${documents.length} documents`)
const authors = await getDocuments({content_type:"authors"})
console.log(`found ${authors.length} authors`)
const generic_markdown = await getDocuments({format:"markdown",content_type:"generic"})
console.log(`found ${generic_markdown.length} generic markdown entries`)

const image_entry = await getEntry({slug:"image"})
const images_urls = image_entry.data.images.map(image=>image.url)
console.log(`'image' content entry has following images '${images_urls}'`)
