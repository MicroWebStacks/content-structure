import {collect} from 'content-structure'
//import {collect} from '../index.js'

await collect({
    rooturl:import.meta.url,
    rel_outdir:"gen"
})
