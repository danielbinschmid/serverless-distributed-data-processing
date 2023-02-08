#!/usr/bin/env bash

export AZURE_STORAGE_ACCOUNT="ermias"
export AZURE_STORAGE_ACCESS_KEY="bY3g1BkV7CLZ8R+0RY4ougqP9wxjnhy3IZGaCn1TyQixCQGsGV0RF5MR9h88HvZNLJj40dUzXtw9+ASt6aMsJA=="
export AZURE_SAS_TOKEN="?sv=2021-06-08&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-02-15T00:52:26Z&st=2023-02-07T16:52:26Z&spr=https&sig=ZD1JxSgtqgTdljMof%2FzGRQjn57wNueCeoqhIHPiy7x0%3D"

connection_string="DefaultEndpointsProtocol=https;AccountName="$AZURE_STORAGE_ACCOUNT";AccountKey="$AZURE_STORAGE_ACCESS_KEY";EndpointSuffix=core.windows.net"

# $1 -> container name
create_azure_containers() {
  az storage container create --name $1 \
    --connection-string $connection_string \
    --sas-token $AZURE_SAS_TOKEN
}

# $1 -> queue name
create_azure_queue() {
  az storage queue create --name $1 \
    --connection-string $connection_string \
    --sas-token $AZURE_SAS_TOKEN
}

# $1 -> container name
# $2 -> file path
upload_file_to_container() {
  file_path="$(basename -- $2)"

  az storage blob upload --file $2 \
    --container-name $1 \
    --name $file_path \
    --connection-string $connection_string \
    --sas-token $AZURE_SAS_TOKEN > /dev/null
}

filelists_container="filelist"
aggregation_jobs_container_name="aggregationjobs"
aggregation_results_container_name="aggregationresults"
merging_jobs_container_name="mergingjobs"
merge_result_container_name="mergeresult"
results_blob_container="results"

aggregation_queue_name="aggregationqueue"
results_queue_name="resultsqueue"
tasks_queue_name="tasksqueue"

#echo "creating containers"
#create_azure_containers $filelists_container
#create_azure_containers $aggregation_jobs_container_name
#create_azure_containers $aggregation_results_container_name
#create_azure_containers $merging_jobs_container_name
#create_azure_containers $merge_result_container_name
#create_azure_containers $results_blob_container
#
#echo "creating queues"
#create_azure_queue $aggregation_queue_name
#create_azure_queue $results_queue_name
#create_azure_queue $tasks_queue_name

#customer_files="./testing_data/customer.[0-9]*.csv"
#
#echo "uploading files to <" $filelists_container "> container"
#for file_path in $customer_files
#do
#  upload_file_to_container $filelists_container $file_path
#done
