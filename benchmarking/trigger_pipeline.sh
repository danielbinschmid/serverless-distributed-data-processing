#!/bin/bash


echo "triggering" $2 "pipeline"

# the output from 'curl' command is discarded remove '> /dev/null' to see the output
trigger_with_multiple_batches() {
  json_files="./json_files/filelist_batch[0-9]*.json"
  echo -e "multiple batches start time:\n" $(date +%H:%M:%S)

  for json_file in $json_files
  do
    #filename="$(basename -- $json_file)"

    echo "triggering with file" $json_file
    curl -X POST $1"/"$2 \
         -H "Content-Type: application/json" \
         -d @$json_file > /dev/null &
    done
}

trigger_with_single_batch() {
  echo -e "single batches start time:\n" $(date +%H:%M:%S)

  filename="./json_files/filelist.json"
  echo "triggering with file" $filename
  curl -X POST $1"/"$2 \
       -H "Content-Type: application/json" \
       -d @$filename > /dev/null
}

trigger_with_multiple_batches "$@"
#trigger_with_single_batch "$@"
