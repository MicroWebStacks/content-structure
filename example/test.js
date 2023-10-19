import {collect} from 'content-structure'

await collect({
    rooturl:import.meta.url,
    rel_outdir:"gen"
})
