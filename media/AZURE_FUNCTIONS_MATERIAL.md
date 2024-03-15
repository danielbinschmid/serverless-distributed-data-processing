## Shared Storage Options

Some of the options for sharing state in the Azure ecosystem are the following:

- Azure Blob Storage: This option allows you to store state in Azure Blob Storage, which is a cloud-based file storage service. This option is suitable for scenarios where you need to store large amounts of data that needs to be persisted across function executions.
- Azure Table Storage: This option allows you to store state in Azure Table Storage, which is a NoSQL key-value store. 
- Azure Queue Storage: This option allows you to store state in Azure Queue Storage, which is a cloud-based message queue service. This option is suitable for scenarios where you need to store state that needs to be persisted across function executions and is not sensitive to high latency. 
- Azure Redis Cache: This option allows you to store state in Azure Redis Cache, which is a in-memory data store that supports various data structures. This option is suitable for scenarios where you need to store state that needs to be persisted across function executions and requires low latency.
- Azure Cosmos DB: This option allows you to store state in Azure Cosmos DB, which is a NoSQL database that supports a variety of data models and APIs. 

## Trigger Options

There are several types of triggers that can be used to invoke Azure Functions. Some of them are the following:

- Blob storage triggers: These triggers allow you to respond to changes in Azure Blob storage, such as the creation of a new blob or the update of an existing blob.
- Queue storage triggers: These triggers allow you to process messages from an Azure Queue storage queue.
- Timer triggers: These triggers allow you to schedule your functions to run at a specific time or on a specific schedule.
- HTTP triggers: These triggers are activated by HTTP requests. They can be used to create APIs or to respond to webhooks.

More on Triggers (https://learn.microsoft.com/en-us/azure/azure-functions/functions-bindings-storage-blob-trigger?tabs=in-process%2Cextensionv5&pivots=programming-language-python)