import {collect} from 'content-structure'
import {fileURLToPath} from 'url';
import {dirname,join} from 'path'
import Database from 'better-sqlite3';

const rootdir = dirname(fileURLToPath(import.meta.url))

await collect({
    rootdir:rootdir,
    contentdir:join(rootdir,"content"),
    file_link_ext:["svg","webp","png","jpeg","jpg","xlsx","glb"],
    outdir:join(rootdir,".structure"),
    debug:false
})

const dbPath = join(rootdir,".structure","structure.db");
try{
    const db = new Database(dbPath,{readonly:true});
    const tables = db.prepare(`
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    `).all();
    console.log("Structure DB tables and row counts:");
    if(tables.length === 0){
        console.log("  (no tables found)");
    }else{
        // Each table count is shown so manual verification is easy.
        for(const {name} of tables){
            const {count} = db.prepare(`SELECT COUNT(*) as count FROM "${name}"`).get();
            console.log(`  - ${name}: ${count}`);
        }
    }
    db.close();
}catch(error){
    console.error(`Failed to read structure database at ${dbPath}: ${error.message}`);
}
