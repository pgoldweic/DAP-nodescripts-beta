# Canvas Data 2 (DAP) data loading scripts

This project includes 2 scripts which handle snapshot and incremental retrievals of Canvas Data 2 respectively. Files are being downloaded locally to a configured base folder. Each run of the snapshots retrieval script creates a new subfolder named 'snapshots-\<timestamp\>', while runs of the incrementals retrieval script create a subfolder named 'incrementals-\<timestamp\>'. The Incrementals script includes logic for finding the most recent file/s retrieved in previous runs for each particular table (it traverses folders previously created as it searches), extracting the timestamp of those files, and using those timestamps as the 'since' value in the next incremental retrieval of each table.  

## Installation
Type:

    npm install

## Configuration
Copy the .env.example file into your own .env file and update contents with appropriate values for your institution.
At the least, you should insert your CD2 api key value as the value for  'CD2ApiKey' in this file. All other parameters are optional. The following are default values for those parameters:

    - sleepMilliseconds: 10000 (number of milliseconds to wait while polling for job status)
    - maxSimultaneousQueries = 10 (controls the number of retrieval jobs -one per table- that are created simultaneously. The next batch of jobs will not be created until the running batch has finished downloading).
    - topFolder:. (determines the base folder for all downloads - by default it uses the current folder).
    - includeSchemaVersionInFilenames = false (controls whether file names include schema version numbers)

## Usage
To run a full set of snapshot retrievals, type:

    npm run snapshots 

To run a full set of incremental retrievals, type:

    npm run incrementals  