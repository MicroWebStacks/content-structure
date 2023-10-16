# content-structure
Content Structure collect all your markdown files meta data and parses the Abstract Syntax Tree of each file

# usage
```javascript
import {collect} from 'content-structure'

await collect({
    rooturl:import.meta.url,
    rel_outdir:"dist"
})
```
config parameters :
* `rooturl` : the url of the root directory that contains a `content` directory
* `rel_outdir` : relative output directory is the location where all output data will be generated, which is relative to the root directory.

