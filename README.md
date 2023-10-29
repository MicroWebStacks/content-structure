# Content Structure
content-structure collects all your markdown files meta data and parses the Abstract Syntax Tree of each file

! note this project is just starting and is not yet generating all planned artefacts !

# Usage
```shell
npm install content-structure
```
collect all data by running this once
```javascript
import {collect} from 'content-structure'

await collect()
```
then use as follows
```javascript
import {get_documents} from 'content-structure'

const documents = await get_documents()
console.log(`\nobtained ${documents.length} documents`)

const authors = await get_documents({content_type:"authors"})
console.log(`\nfound ${authors.length} authors`)

const generic_markdown = await get_documents({format:"markdown",content_type:"generic"})
console.log(`\nfound ${generic_markdown.length} generic markdown entries`)
```
will output
```shell
obtained 14 documents

found 3 authors

found 11 generic markdown entries
```

# Roadmap
- [x] provide an API for querying documents content-by-x
- [ ] provide an API for querying image-by-x, table-by-x,...
- [ ] helper for search engine injection
- [ ] extracting svg text and span content with jsdom
- [ ] test hierarchical content
- [ ] check compatibility with content-collections
- [ ] add optional typecheck


# Documentation
## Documents fields description
### Content type
Content type is a field existing in every document as `content_type`. For a hierarchically structured content, the content type can be derived from the parent folder, and can in all cases be overridden by the user when defined in the meta data (markdown frontmatter or content of json or yaml)

 1. `content_type` field in data  => taken from data
 2. content depth > 1             => type derived from the parent folder
 3. root content                  => generic

The "generic" content type is the default assignment when no parent and no manual type is provided, the genric type does not get included in the uid definition

The content type, like any other field, can be filtered as follows
```javascript
const authors = await get_documents({content_type:"authors"})
```
see also the following section for an `author` content_type example.

### URL type
Content structure allows both file and folder URL types to be used at the same time without the need of user configuration. The convention is the basename of the file, in case it is `readme` for markdown or `entry`,`document` for json or yaml, the URL will be considered as folder, and file for any other filename.

All of the three files below will automatically generate a filed `content_type` of the parent folder `authors` if not otherwise specified inside the json or yaml files.
```shell
 ───content
    ├───authors
    │   │   myself.json
    │   │   stephen-king.yaml
    │   └───agatha-christie
    │           entry.yml    ...
```
the field `url_type` will also be exposed for the user as in the example entry below
```json
  {
    "sid": "a518c9b7",
    "uid": "authors.agatha-christie",
    "path": "authors/agatha-christie/entry.yml",
    "url_type": "dir",
    "slug": "agatha-christie",
    "format": "data",
    "content_type": "authors",
    ...
  }
```

## Config parameters
the config parameter is optional and do have default values
* `rootdir` : defaults to current working directory. The path where to find the a `content` directory.
* `rel_outdir` : defaults to `gen`. Relative output directory is the location where all output data will be generated, which is relative to the root directory.

## Generated output
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

## Example generated output

this files structure
```shell
└───content
    ├───title-complex
    │       readme.md
    ├───text-simple
    │       readme.md
    ...
```
generates this output
```shell
└─gen
  │   index.json
  └───documents
      ├───35298154
      │       content.json
      │       tree.json
      ├───12b0e722
      │       content.json
      │       tree.json
      ...
```
* `index.json` is the documents index
```json
[
  {
    "sid": "35298154",
    "uid": "title-complex",
    "path": "title-complex/readme.md",
    "url_type": "dir",
    "slug": "title-complex",
    "format": "markdown",
    "title": "title Complex",
    "content_type": "generic"
  },
  {
    "sid": "12b0e722",
    "uid": "text-simple",
    "path": "text-simple/readme.md",
    "url_type": "dir",
    "slug": "text-simple",
    "format": "markdown",
    "title": "Text Simple",
    "content_type": "generic"
  },
  ...
```
* file content example
```markdown
---
title: Image
---
![Tree](./tree.svg)

```
example of generated files for `image/readme.md` which has an sid of `78805a22`
```json
{
  "sid": "78805a22",
  "uid": "image",
  "path": "image/readme.md",
  "url_type": "dir",
  "slug": "image",
  "format": "markdown",
  "title": "Image",
  "content_type": "generic",
  "headings": [],
  "tables": [],
  "images": [
    {
      "id": "tree",
      "heading": null,
      "title": null,
      "url": "./tree.svg",
      "alt": "Tree",
      "text": ""
    }
  ],
  "code": [],
  "paragraphs": [
    {
      "heading": null,
      "text": []
    },
    {
      "heading": null,
      "text": []
    }
  ]
}```
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