import {collect} from 'content-structure'
import {fileURLToPath} from 'url';
import {dirname,join} from 'path'
import Database from 'better-sqlite3';

const rootdir = dirname(fileURLToPath(import.meta.url))
const abs_db_path = join(rootdir,".structure/structure.db")


await collect({
    rootdir: rootdir,
    contentdir: join(rootdir,"content"),
    file_link_ext: ["svg","webp","png","jpeg","jpg","xlsx","glb"],
    outdir: join(rootdir,".structure"),
    db_path: abs_db_path,
})


try{
    const db = new Database(abs_db_path,{readonly:true});
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
    console.error(`Failed to read structure database at ${abs_db_path}: ${error.message}`);
}
