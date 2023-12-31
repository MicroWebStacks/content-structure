import { visit } from 'unist-util-visit';

function text_match(text,match_regex){
    let result = new Set()
    const regex = new RegExp(match_regex, 'g');
    const matches = text.matchAll(regex);
    let hasMatches = false;
    for(const match of matches){
        hasMatches = true
        result.add(match[1])
    }
    return [...result]
}

function visit_node_match(node,index, parent,match_key,match_regex){
    let new_nodes = []
    const regex = new RegExp(match_regex, 'g');
    const matches = node.value.matchAll(regex);
    let hasMatches = false;
    let current_index = 0
    for(const match of matches){
        hasMatches = true
        if(match.index > current_index){
            new_nodes.push({
                type:"text",
                value:node.value.substring(current_index,match.index),
                //known limitation using parent line which should count nb \n
                //this limitation does not interfere with the usage for paragraphs identification
                position:{start:{line:node.position.start.line}}
            })
        }
        new_nodes.push({
            type:"reference",
            ref_type:match_key,
            ref_value:match[1],
            position:{start:{line:node.position.start.line}},
            children:[{type:"text",value:match[1],
                        position:{start:{line:node.position.start.line}},
                    }]
        })
        current_index = match.index + match[0].length
    }
    if(hasMatches){
        if(current_index < node.value.length){
            new_nodes.push({
                type:"text",
                value:node.value.substring(current_index,node.value.length),
                position:{start:{line:node.position.start.line}}
            })
        }
        //console.log(JSON.stringify(new_nodes))
        parent.children.splice(index, 1, ...new_nodes);
    }
    return node;
}

function remarkMatches(tree,matches) {
    visit(tree,'text',(node, index, parent)=>{
        for(const match in matches ){
            if(node.value){
                node = visit_node_match(node,index, parent, match,matches[match])
            }
        }
        return node
    }) 
    return tree
}

function textListMatches(textList,matches){
    let result = []
    for(const match in matches){
        for(const text of textList){
            const text_hits = text_match(text,matches[match])//every text entry can have many matches
            for(const hit of text_hits){
                result.push({
                    type:match,
                    value:hit
                })
            }
        }
    }
    return result
}

export{
    remarkMatches,
    textListMatches
}
