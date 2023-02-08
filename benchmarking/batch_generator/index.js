
const fs = require('fs')


const outputName = "./filelist_json" + Math.random() + ".csv";

const filelist = []

for (var i = 0; i < 100; i++) {
    var num_prefix = i < 10? "0": ""; 
    filelist.push("customer." + num_prefix + i + ".json");
}

const filelistJson = {
    "filelist": filelist
}


const jsonString = JSON.stringify(filelistJson)
fs.writeFile(outputName, jsonString, err => {
    if (err) {
        console.log('Error writing file', err)
    } else {
        console.log('Successfully wrote file')
    }
})