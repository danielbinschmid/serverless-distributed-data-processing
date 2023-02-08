#!/bin/bash

leading_zero(){
    local num=$1
    local zeroos=000
    echo ${zeroos:${#num}:${#zeroos}}${num} 
}

split_csv_files_into_batches_json() {
  num_batches=$(($1 / $2))
  for ((i = 0; i < $2; i++)); do
    filename="./json_files/filelist_batch"$i".json"
    for ((j = 0; j < $num_batches; j++)); do
      num=$((i * $num_batches + j))
      echo "customer.$(printf %03d $num).csv"
    done | jq -R . | jq -s '{"filelist": .}' > $filename
  done
}

single_batch_json() {
  filename="./json_files/filelist.json"
  for ((i = 0; i < $1; i++)); do
    echo "customer.$(printf %03d $i).csv"
  done | jq -R . | jq -s '{"filelist": .}' > $filename
  
}

split_csv_files_into_batches_json "$@"
#single_batch_json "$@"
