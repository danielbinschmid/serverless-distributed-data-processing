#!/bin/bash

# USAGE: API-URL <pipeline_type> <filelist-json (file.name)>

curl -d @$3 -X POST $1$2 -H "Content-Type: application/json" 

