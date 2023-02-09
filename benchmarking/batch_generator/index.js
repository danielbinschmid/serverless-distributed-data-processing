
const fs = require('fs')


function gen(start, end, nameID, folder) {
    const outputName = "./" + folder + "/filelist_" + nameID + "_" + Math.random() + ".json";

    const filelist = []

    for (var i = start; i < end; i++) {
        var num_prefix = i < 10? "0": ""; 
        num_prefix += i < 100? "0": "";
        filelist.push("customer." + num_prefix + i + ".csv");
    }

    const filelistJson = {
        "filelist": filelist
    }


    const jsonString = JSON.stringify(filelistJson)

    fs.mkdir("./" + folder, (err) => {
        if (err) {
            return console.error(err);
        }
        console.log('Directory created successfully!');
    });
    fs.writeFile(outputName, jsonString, err => {
        if (err) {
            console.log('Error writing file', err)
        } else {
            console.log('Successfully wrote file')
        }
    })
}

function genBatches(nBatches, folder) {
    const totalFiles = 500;
    const filesPerBatch = totalFiles / nBatches;
    for (var i = 0; i < nBatches; i++) {
        gen(filesPerBatch * i, filesPerBatch * (i + 1), "_batch_" + i, folder);
    }
}


const nBatches = [1, 5, 10, 20, 50, 100, 250, 500];

for (const batch of nBatches) {
    genBatches(batch, "nBatches_" + batch);
} 


