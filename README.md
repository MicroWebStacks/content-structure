# content-structure
Content Structure collect all your markdown files meta data and parses the Abstract Syntax Tree of each file

! note this project is just starting and is not yet generating all planned artefacts !

# usage
```javascript
import {collect} from 'content-structure'

await collect({
    rooturl:import.meta.url,
    rel_outdir:"gen"
})
```
config parameters :
* `rooturl` : the url of the root directory that contains a `content` directory
* `rel_outdir` : relative output directory is the location where all output data will be generated, which is relative to the root directory.

# output
* `gen/index.json`
    * documents : a list of documents properties
        * slug : auto generated if not provided
        * uid : autogenerated and unique across all documents
        * sid : a short uid with first 8 letters of the md5 hash, for simplified referencing e.g. in data directories or links
    * images : a list of images properties. These images were parsed from the markdown text content and not from the filesystem
        * heading : the heading id of the section the image belongs to
        * title : from the image link meta data
        * document : the document the image was referenced in
* each markdown file gets a `./gen/documents/<sid>` directors with
    * `tree.json` the raw output of the remark AST parser
    * content.json with the parameters and parsed content parameters

# example
this files structure
```shell
├───image
│       readme.md
│       tree.svg
├───table-simple
│       readme.md
├───text-simple
│       readme.md
```
with as example the content of `image/readme.md`
```markdown
---
title: Image
---
![Tree](./tree.svg)
```
generates this output in the output `./gen/` folder
```shell
│   index.json
│   
└───documents
    │       
    ├───78805a22
    │       content.json
    │       tree.json
    │
    ...
```
* `index.json` content example
```json
{
  "documents": [
    {
      "title": "Text Simple",
      "path": "text-simple/readme.md",
      "content_type": "generic",
      "url_type": "dir",
      "slug": "text-simple",
      "uid": "text-simple",
      "sid": "12b0e722"
    },
    {
      "title": "Table Simple",
      "path": "table-simple/readme.md",
      "content_type": "generic",
      "url_type": "dir",
      "slug": "table-simple",
      "uid": "table-simple",
      "sid": "b08ef064"
    },
    {
      "title": "Image",
      "path": "image/readme.md",
      "content_type": "generic",
      "url_type": "dir",
      "slug": "image",
      "uid": "image",
      "sid": "78805a22"
    }
  ],
  "images": [
    {
      "id": "tree",
      "heading": null,
      "title": null,
      "url": "./tree.svg",
      "alt": "Tree",
      "document": "78805a22"
    }
  ]
}
```
* example of generated files for `image/readme.md` which has an sid of `78805a22`
```json
{
  "title": "Image",
  "path": "image/readme.md",
  "content_type": "generic",
  "url_type": "dir",
  "slug": "image",
  "uid": "image",
  "sid": "78805a22",
  "headings": [],
  "tables": [],
  "images": [
    {
      "id": "tree",
      "heading": null,
      "title": null,
      "url": "./tree.svg",
      "alt": "Tree",
      "document": "78805a22"
    }
  ],
  "code": [],
  "paragraphs": []
}
```
and the beginning of `tree.json`
```json
{
  "type": "root",
  "children": [
    {
      "type": "paragraph",
      "children": [
        {
          "type": "image",
          "title": null,
          "url": "./tree.svg",
          "alt": "Tree",
          "position": {
            "start": {
              "line": 1,
              "column": 1,
              "offset": 0
            },
...
```
