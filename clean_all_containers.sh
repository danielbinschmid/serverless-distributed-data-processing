#!/bin/bash
for value in filelists aggregationjobs aggregationresults mergingjobs mergeresult
do
	echo "Cleaning container with name: $value"
	az storage blob delete-batch --account-name $1 --source $value --connection-string $2
	echo "-- DONE --"
done
