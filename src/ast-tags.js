import { visit } from 'unist-util-visit';

function remarkTags(tree) {
    visit(tree,'text',(node, index, parent)=>{
        if (node.children) {
            node.children = node.children.map(visitNode);
        }
        if (node.type === 'text') {
            console.log(node.value)
            const regex = /page::(\w+)/g;
            const match = node.value.match(regex);
            if(match){
                const page = RegExp.$1
                console.log(` => found page:: (${page})`)
                const tagNode = {
                    type: 'tag',
                    page: page,
                    children: [{ type: 'text', value: `page(${page})` }],
                }
                parent.children.splice(index, 1, tagNode)
            }
        }
        return node;
    }) 
    return tree
}

export{
    remarkTags
}
