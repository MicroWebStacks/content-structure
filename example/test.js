import {collect, get_documents} from 'content-structure'
import {fileURLToPath} from 'url';
import {dirname} from 'path'

await collect({
    rootdir:dirname(fileURLToPath(import.meta.url)),
    rel_contentdir:"content",
    rel_outdir:"gen",
    debug:true
})

const documents = await get_documents()
console.log(`\nobtained ${documents.length} documents`)
