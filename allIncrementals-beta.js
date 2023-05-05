require('dotenv').config();

const base_URL = process.env.dap_URL
const dap_URL = base_URL + "/dap"

const axios = require('axios').default;
const sleepMs = process.env.sleepMilliseconds || 10000

const Fs = require('fs')  
const fs = require('fs').promises;
const path = require('path')
const Https = require('https')

// request body (parameters) used in an auth request 
const authData = { grant_type:'client_credentials'}

// authorization endpoint
const authEndpoint = base_URL + "/ids/auth/login"

// job monitoring endpoint
const pollJobEndpointBase  = dap_URL + "/job/"  

// table listing endpoint
const tableListingEndpoint = dap_URL + "/query/canvas/table"

// maximum number of simultaneous queries to be sent to the DAP service
const maxSimultaneousQueries = process.env.dap_maxSimultQueries || 10

// will hold the currently valid auth token
var currentlyValidToken = undefined
var currentlyValidTokenResponse 

const jwts = require('jsonwebtoken')
const querystring = require('querystring')

const defaultTopFolder = process.env.topFolder || "."
console.log("Top folder for file storage is: ", defaultTopFolder)

/** Returns an authentication token using authData as parameters to the request
 * 
 * <p> if 'complete' is true, then the function returns an object with format { access_token: blah, expires_at: blah }. 
 * Otherwise, a plain string (representing the auth token) is returned. </p>
 * 
 * <p> As of 12/20/22, we augment the response of the Auth service with an 'expires_at' field. Note however that the
 * expires_at value can also be obtained from the token itself when decoding it (see ensureValidToken below).</p>
 */
const obtainAuth = async (authEndpoint, authData, complete) =>  {
	try {
		const response =  await axios({
			method: 'POST',
			url: authEndpoint,
			auth: { username: process.env.CD2ClientID, password: process.env.CD2Secret}, // 5/5/23
			data: querystring.stringify(authData), 
			// 5/5/23 headers: {"Content-Type": "application/x-www-form-urlencoded", "Authorization": "Basic " + 
			// 5/5/23 process.env.CD2ApiKey} // 12/7/22
			headers: {"Content-Type": "application/x-www-form-urlencoded"} // 5/5/23
		})
		//console.log("Obtained response from Axios: ", response)
		//console.log("Obtained response from Axios with .json form: ", response.json())
		if (response && response.data && response.data.access_token) {
			console.log("Successfully obtained auth token at ", new Date())
			currentlyValidToken = response.data.access_token // refresh current (global) token
			if (response.data && !response.data.expires_at) {// estimates 'expires_at' as 1 hour minus 1 seconds from now
				response.data["expires_at"] = Date.now() + (response.data.expires_in - 1) * 1000
			}
			currentlyValidTokenResponse = response.data
			console.log("Just refreshed globally valid token with newly obtained one... ")
			return complete ? response.data : response.data.access_token
		}
	} catch(error) {
		console.log("Obtained error from Axios when requesting auth token: ", error)
		console.error(error, error.stack)
		return undefined
	}
}

/** Returns the given token if it expires in 5 minutes or more, or a newly obtained token otherwise 
 *  
 *  <p> Works with either a string or an object input of the form { access_token, expires_at, ....} </p>
 */
const ensureValidToken = async (tokenResponse) => {
	if (tokenResponse.access_token) {// assume that input is a complete auth response object
		if (tokenResponse.expires_at) {
			let expires = new Date(tokenResponse.expires_at)
			if (Date.now() + (5*60*1000) > expires.getTime()) {// token will expire soon --> re-obtain
				//console.log("Will obtain new token since original may have expired")
				let result = await obtainAuth(authEndpoint, authData, true) // return object with new token in it
				if (result) {
					//console.log("Obtained new auth token because original may have expired - old was: ", tokenResponse)
					//console.log("New one is: ", result)
					return result
				}
			
			} else {
				return tokenResponse // return the given input
			}
		} else {// no expiration present --> warn and return same token response
			console.log("Warning! Cannot determine validity of token with no expiration date!!")
			return tokenResponse			
		}
	} else {// assume that input is an auth token string and NOT a complete auth response
		let decoded = jwts.decode(tokenResponse)
		//console.log("Here's my decoded token header: ", decoded.header)
		//console.log("Here's my decoded token header: ", decoded.payload)
		// console.log("here's my decoded token: ", decoded)
		if (Date.now() + (5*60*1000) > (decoded.exp * 1000)) {// token will expire within the next 5 minutes --> re-obtain
			let result = await obtainAuth(authEndpoint, authData) // return new token string
			if (result) {
				//console.log("Obtained new auth token because original may have expired - old was: ", tokenResponse)
				//console.log("New one is: ", result)
				return result
			}
		} else {
			return tokenResponse // return the given input token
		}
	}
}

/** Starts a DAP table retrieval job and returns job information
 * 
 * <p>Format is { "id": "<jobidstring>", "status": "running", "started_at": "<iso-UTC-timestamp>" }
 */
const retrieveTable = async (table, format, authResponse, filter, since, until) =>  {
	// initialize query params
	if (authResponse) {
		authResponse = await ensureValidToken(authResponse)
	} else {
		authResponse = await ensureValidToken(currentlyValidToken)
	}
	if (authResponse) {
		let authToken = authResponse.access_token ? authResponse.access_token : authResponse		
		let authHeaders = { "x-instauth": authToken } 
		let queryParams = { "format": format || 'jsonl' }
		if (since) queryParams["since"]= since
		if (until) queryParams["until"]= until
		if (filter) queryParams["filter"]= filter
		let queryTableEndpoint = dap_URL + "/query/canvas/table/" + table + "/data" 
		console.log("Table to be queried: " + table + " using settings: ", queryParams)
		
		try {
			const response = await axios( { method: 'POST', url: queryTableEndpoint, data: queryParams, 
				headers: authHeaders})
			if (response && response.data) {
				//console.log("Obtained response to job creation: ", response.data)
				console.log("Job " + response.data.id + " was successfully created for retrieval of " + table + " with " + 
					(response.data.objects ? response.data.objects.length : 'NO') + " objects associated to it")
				return response.data
			}
		} catch (error) {
			console.log("Obtained error from Axios when creating retrieval job: ", error)
			console.error(error, error.stack)
			throw error
		}
	}
}

/** Monitors a job which has already started running given its Id and an authentication token
 * 
 */
const monitorJob = async (jobId, authResponse) => {
	//authResponse = ensureValidToken(authResponse)
	//let jwt = authResponse.access_token ? authResponse.access_token : authResponse
	let jwt
	let authHeaders
	let pollJobEndpoint = pollJobEndpointBase + jobId
	//let authHeaders = { "Authorization": "Bearer " + jwt } 
	//console.log("Monitoring job: ", jobId)
	let jobStatus = "running"
	let result = undefined
	while (jobStatus === "running" || jobStatus === "waiting") {
		if (authResponse) {
			authResponse = await ensureValidToken(authResponse)
		} else {
			authResponse = await ensureValidToken(currentlyValidToken)
		}
		if (authResponse) {
			//console.log("Polling job: ", jobId)
			jwt = authResponse.access_token ? authResponse.access_token : authResponse
			authHeaders = { "x-instauth": jwt } 
			try {
				let response = await axios( { method: 'GET', url: pollJobEndpoint, headers: authHeaders})
				if (response && response.data) {
					jobStatus = response.data.status
					if (jobStatus !== "running" && jobStatus !== "waiting") {
						console.log("Job: " + jobId + "has either completed or failed: ", 
								response.data)
						result = response.data
						break;
					} else {
						console.log("Job: " + jobId + " is still " + jobStatus + "...")
						await delay(sleepMs)
					}
				}
			} catch(error) {
				console.log("Oops! Job may have failed!:", jobId)
				console.log("Obtained error from Axios when polling job status: ", error)
				console.error(error, error.stack)
				throw error
			}
		}
	}
	return result
}

/** Retrieve data from URLs given within responseData, and with a given auth token
 * 
 */
const retrieveObjectURLs = async (responseData, authResponse)=> {
	const {id, status, at, objects} = responseData
	if (authResponse) {
		authResponse = await ensureValidToken(authResponse)
	} else {
		authResponse = await ensureValidToken(currentlyValidToken)
	}
	if (authResponse) {
		let jwt = authResponse.access_token ? authResponse.access_token : authResponse
		let authHeaders = { "x-instauth": jwt } 
		console.log("Will now retrieve data for completed job: " + id + " which has ended at: " + at)
		let endpointObjectsList = dap_URL + "/object/url"
		try {
			let response = await axios( { method: 'POST', url: endpointObjectsList, headers: authHeaders, 
				data: objects})
			if (response && response.data) {
				console.log("Obtained object URLs response as follows: ", response.data)
				return response.data.urls
			}
		} catch(error) {
			console.log("Oops! Could NOT retrieve object URLs !:", responseData)
			console.log("Obtained error from Axios when retrieving object URLs: ", error)
			console.error(error, error.stack)
			throw error
		}
	}
}

/** Locally downloads to 'folderName' all the file urls given for'table', which uses schema 'schema_version'
 * <p> 'urls' is of the form: {
		  'part-0000-blah.json.gz': {
			    url: 'https://data-access-platform-output-prod-iad.s3.amazonaws.com/output/rootAccountId%3DWTbP67mC863Zx9qZ1XtqGSxhiLCO5sjJQ2lPGfgf/queryId%3D5293121f-6250-4aa4-b050-2e4c5e5ac645/part-0000-blah.json.gz?X-Amz-Security-Token=FwoGZXIvYXdzEHwaDIxZDeQmXcZnSr57rCK7Ab8UcDOkeegNuTuA%2B0xHHddbR1%2Bmcy2prq2MIMigBV3ItGLmnpRBxb0i%2B%2FW37WJjW%2FhRUcj9FzGEHGlzhm2TnHE41TYRWelAHAsNtBNqPDUIaZlyxOU6jBmihEaHbS6O0PxNunNTFrs1UI3gRgekvkpvZOnBlXmzd1eENNUWyKtYOLPm0kChPY0h73UYcuyn4O0cR27SopjIoYnX0bWxJGYxdOZ70f%2BZ3yg9VQ9QsViqsZ7qZijpw58znysoirPemAYyLb10UK4BqtD5LJzeaexFf%2BPsfmpW6WsToy%2BkSdajKw6jD0DxoO2xxa3apfbDmg%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220906T190646Z&X-Amz-SignedHeaders=host&X-Amz-Expires=3600&X-Amz-Credential=ASIAXX2PINZLDE5NNTHX%2F20220906%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=7497dc9941bf5ae73cf73c8b3def019e15f55973bb658f2ca4fcb41efa950cda'
			  }
			}
 */
const downloadAllData = async (urls, table, at, folderName, schema_version) => {
	// Ensure folder exists
	ensureDirExists(folderName)
	// Download Data to the specified folder
	at = at.replaceAll(':','-')
	console.log("Will download all incremental data as of: " + at + " for table: " + table + " into folder: " + folderName + " for schema version: ", schema_version)
	let allPromises = []
	for (let objectId in urls) {
		try {
			if (urls.hasOwnProperty( objectId)) {
				let urlObject = urls[objectId]
				let fileUrl = urlObject.url
				let fileNameTokens = fileUrl.split("/")
				let fileName = table + 
					(process.env.includeSchemaVersionInFilenames && 
						process.env.includeSchemaVersionInFilenames === "true" ? ("_v" + schema_version ) : "") + 
							"_" + at + "_" // 12/20/22 now recording schema_version depending on configuration parameter
				// let fileName = table + "_v" + schema_version + "_" + at + "_" // 11/18/22 now recording schema_version
				if (fileNameTokens && fileNameTokens.length) {
					fileName += fileNameTokens[fileNameTokens.length -1]
					let params = fileName.indexOf("?")
					if (params >=0) fileName = fileName.substring(0, params)
					
				}
				if (fileName) {
					let pathname = folderName ? folderName + "/" + fileName : fileName
					//console.log("About to download data from url: " + fileUrl + " onto: " + fileName)
					console.log("Downloading data from url: " + fileUrl + " onto path: " + pathname + " for table: " + table + " and schema version: ", schema_version)
					//allPromises = allPromises.concat([downloadFile(fileUrl, fileName)])
					allPromises = allPromises.concat([downloadFile(fileUrl, pathname)])
				}
			}
		} catch (error) {
			console.log("Error creating download promise for object: " + objectId + " for table: " + table, error)
			console.error(error, error.stack)
			throw error
		}
	}
	//console.log("Here is the set of file download promises I've prepared for table: " + table + " as of: " + at, allPromises)
	console.log("Prepared " + allPromises.length + " download promises for table: " + table + " as of (recorded) time ", at)
	try {
		return await Promise.all(allPromises)
	} catch (error) {
		console.log("Error downloading file/s! for table: " + table, error)
		console.error(error, error.stack)
		throw error
	}
}

/** Downloads a given URL onto a local file
 * 
 * @param url
 * @param targetFile
 * @returns
 */
async function downloadFile (url, targetFile) {  
	  return await new Promise((resolve, reject) => {
	    Https.get(url, response => {
	      const code = response.statusCode ?? 0

	      if (code >= 400) {
	        return reject(new Error(response.statusMessage))
	      }

	      // handle redirects
	      if (code > 300 && code < 400 && !!response.headers.location) {
	        return downloadFile(response.headers.location, targetFile)
	      }

	      // save the file to disk
	      const fileWriter = Fs
	        .createWriteStream(targetFile)
	        .on('finish', () => {
	        	console.log("Finished writing: " + targetFile + " by time: ", new Date())
	        	resolve({})
	        })
	      response.pipe(fileWriter)
	    }).on('error', error => {
	    	console.log("File download for url: " + url + " and target file: " + targetFile + " produced an error!", error)
	    	console.error(error, error.stack)
	    	reject(error)
	    })
	  })
	}



function delay(time) {
	  return new Promise(resolve => setTimeout(() => resolve(true), time));
} 

/** Retrieves a table (using starting timerange if available) and downloads it onto the local disk's given pathname
 * 
 * NOTES: one could potentially run some specialized script if lastSchemaVersion and the new version retrieved 
 * (in 'schema_version) differ
 * 
 */
const retrieveCompleteTable = async(table, since, until, folderName, lastSchemaVersion) => {
	console.log("Getting started with retrieval of data for table: " + 
			table + (since ? " since "+ since : " from the beginning of time") + 
			(until ? " till " + until : " till now"))
	let monitoringData
	let result
	try {
		let authResponse = currentlyValidToken ? 
				await ensureValidToken(currentlyValidToken) : 
				await obtainAuth(authEndpoint, authData)
		if (authResponse) {
			//let jwt = await obtainAuth(authEndpoint, authData)
			let job = await retrieveTable (table, "jsonl", authResponse, undefined, since, until)
			if (job) {
				const {id, status, at, since, until, schema_version, objects} = job
				if (status === "failed") {
					throw new Error("Retrieval job immediately terminated with a 'failed' status for table: " + 
							table + " at " + new Date())
				}
				if (status === "complete") {
					let endedAt = at ? at : until // depends on the type of job run
					console.log("Great! Retrieval job: " + id + " completed successfully and is current as of " + endedAt + 
									" and has " + (objects ? objects.length : 'NO') + " objects associated to it!")
					let urlsRetrieved = await retrieveObjectURLs(job)
					if (urlsRetrieved) {// retrieve each object via their URL
						console.log("Will now retrieve the following object URLs:", urlsRetrieved)
						result = await downloadAllData(urlsRetrieved, table, endedAt, folderName, schema_version)
						if (result) {
							console.log("Yay! I downloaded all the data for table: " + table + " by time: " + new Date())
						}
						return result
					}
				} else {// status is running or waiting
					if (id && (status === "running" || status === "waiting")) {// need to wait until job completion
						console.log("Starting to monitor job: ", id)
						// 10/27/22 monitoringData = await monitorJob (id, jwt)
						monitoringData = await monitorJob (id)
						if (monitoringData) {// job completed 
							const {id2, status, at, since, until, schema_version, objects} = monitoringData
							//let jobMode = at ? "snapshot" : "incremental"
							let endedAt = at ? at : until // depends on the type of job run
							if (status === "complete") {
								//console.log("Great! Retrieval job: " + id2 + " as of: " + endedAt + " for table: " + table + " completed successfully!")
								console.log("Great! Retrieval job: " + id + " completed successfully and is current as of " + endedAt + 
									" and has " + (objects ? objects.length : 'NO') + " objects associated to it!")
								// 10/27/22 let urlsRetrieved = await retrieveObjectURLs(monitoringData, jwt)
								let urlsRetrieved = await retrieveObjectURLs(monitoringData)
								if (urlsRetrieved) {// retrieve each object via their URL
									console.log("Will now retrieve the following object URLs for table: " + table + ":", urlsRetrieved)
									result = await downloadAllData(urlsRetrieved, table, endedAt, folderName, schema_version)
									if (result) {
										console.log("Yay! I downloaded all the data as of: " + endedAt + " for table:" + table + " by time: ", new Date())
									}
								}
							} else if (status === "failed") {// job failed while monitoring  it
								console.log("Oops! Retrieval job: " + id2 + " for table: " + table + " failed to complete!")
								throw new Error("Table: " + table + " could NOT be retrieved since retrieval job: " + id2 + " failed!")
							}
								
						} 
						return result
					}
				}
			}
		}
	} catch (error) {
		console.log("Catching uncaught exception within retrieveCompleteTable! - unable to retrieve: " + 
				table + " at time: " + new Date())
		console.error(error, error.stack)
		return {error: error, table: table}
	}
}

/** Retrieves the table listing from a local schema file (provided by Instructure: schema.json
 * 
 */
const retrieveTablesSchema = () => {
	const schema = require('./schema.json')
	return schema
}

/** Retrieves incrementally a subset of tables given by an array of table names
 * 
 * <p> For each table, it figures out the last update time for that table -that is, 'current as of time'- 
 * from the filename of the most recently retrieved table file, and uses it as the 'since' value for 
 * a new incremental retrieval job for that table. </p>
 */
const retrieveIncrementalTableSubsetOLD = async(tablesList, folderName, until)=> {
	console.log("Will try to retrieve incremental updates for the following tables... ", tablesList)
	let table
	let errored
	let retrieved
	let promises = []
	// TODO: if until is undefined, consider setting up a reasonable time for it here
	try {
		for (let i=0; i < tablesList.length; i++) {
			table = tablesList[i]
			// find most recent file representing an update to this table
			let recentUpdate = await findLastTableUpdate(table, folderName)
			// parse file name to obtain tokens, including the timestamp of the latest table update
			if (recentUpdate) {
				let fileParts = parseFileName(recentUpdate)
				let since = undefined
				if (fileParts.at) {
					let timestamp = fileParts.at
					let timeparts = timestamp.split("T")
					if (timeparts.length === 2) {
						let date = timeparts[0]
						let time = timeparts[1]
						since = date + "T" + time.replace("-",":")
					}
				}
				promises.push(retrieveCompleteTable(table, since, until, folderName))
			} else {// this table needs to be retrieved for the first time --> use a snapshot
				promises.push(retrieveCompleteTable(table, undefined, until, folderName))
			}
		}
	} catch (error) {
		console.log("Uncaught top level Error while finding the most updated file for table: " + table + 
				"- terminating incremental script!: ", error)
		console.error(error, error.stack)
	}
	try {
		let responses = await Promise.all(promises)
		if (responses) {
			
			console.log("Looks like I've finished retrieving all the tables in the input set!")
			let errors = responses.filter(response => response && response.error && response.table)
			if (errors && errors.length > 0) {
				console.log(errors.length + " errors have occurred during table retrieval as follows: ", errors )
			} else {
				console.log("Yay! all retrievals were successful for all tables in the set!")
			}
			errored = errors.map(errorResponse => errorResponse.table)
			retrieved = tablesList.filter(tableName => 
				!(errored && errored.length > 0 && errored.includes(tableName)))
			console.log("The following tables were successfully retrieved: ", retrieved)
		}
	} catch(error) {
		// NOTE: this should never happen, as each retrieval does NOT throw exceptions
		console.log("Uncaught top level Error - terminating incremental script!: ", error)
		console.error(error, error.stack)
	}
}

const partitionArrayIntoGroups = (inputArray, maxElements) => {
	let result = []
	let currentList = []
	let currentListIndex = 0
	for (let index=0; index < inputArray.length; index++) {
		currentList.push(inputArray[index])
		currentListIndex++
		if (currentListIndex === maxElements) {
			result.push(currentList)
			currentList = []
			currentListIndex = 0
		}
	}
	if (currentList.length > 0) {// there are a few items left to be pushed to the result
		result.push(currentList)
	}
	return result
}

/** Retrieves (incrementally) a subset of tables given by an array of table names
 * 
 */
const retrieveIncrementalTableSubset = async(tablesList, folderName, until)=> {
	console.log("Starting incremental retrievals onto folder: " + folderName + " for the following tables... ", tablesList)
	let table
	let errored
	let retrieved
	let allSuccessfulRetrievals = []
	let allFailedRetrievals = []
	let allErrors = []
	let partitionedTables = partitionArrayIntoGroups(tablesList, maxSimultaneousQueries)
	console.log("An incremental table subset retrieval was partitioned into " + partitionedTables.length + " groups")
	if (partitionedTables.length > 0) {
		//while (!done) { 
		for (let promiseGroupIndex = 0; promiseGroupIndex < partitionedTables.length; promiseGroupIndex++) {
			// submit in sequence all promise subgroups
			console.log("Partition retrieval iteration now starts for group: ", promiseGroupIndex)
			//console.log("Will try to retrieve all tables in partition group: " + promiseGroupIndex)
			console.log("Tables in this subgroup are: ", partitionedTables[promiseGroupIndex])
			let promises = []
			// TODO: if until is undefined, consider setting up a reasonable time for it here
			try {
				for (let i=0; i < partitionedTables[promiseGroupIndex].length; i++) {
					table = partitionedTables[promiseGroupIndex][i]
					// find most recent file representing an update to this table
					let recentUpdate = await findLastTableUpdate(table, folderName)
					// parse file name to obtain tokens, including the timestamp of the latest table update
					if (recentUpdate) {
						let fileParts = parseFileName(recentUpdate)
						let since = undefined
						if (fileParts.at) {
							let timestamp = fileParts.at
							let timeparts = timestamp.split("T")
							if (timeparts.length === 2) {
								let date = timeparts[0]
								let time = timeparts[1]
								since = date + "T" + time.replace(/\-/g,':')
								console.log("Computed since value as: ", since)
							}
						}
						promises.push(retrieveCompleteTable(table, since, until, folderName, fileParts.schema_version))
					} else {// this table needs to be retrieved for the first time --> use a snapshot
						promises.push(retrieveCompleteTable(table, undefined, until, folderName))
					}
				}
			} catch (error) {
				console.log("Uncaught top level Error while finding the most updated file for table: " + table + 
						"- terminating incremental script!: ", error)
				console.error(error, error.stack)
			}
			// promises holds all the table retrieval promises for this partition's group
			try {
				let responses = await Promise.all(promises)
				while (!responses) {
					await delay(sleepMs)
				}
				if (responses) {
					console.log("Finished retrieving all the tables in group: ", promiseGroupIndex)
					let errors = responses.filter(response => response && response.error && response.table)
					if (errors && errors.length > 0) {
						console.log(errors.length + " errors have occurred during table retrieval as follows: ", errors )
					} else {
						console.log("Yay! all subgroup retrievals were successful in group: ", promiseGroupIndex)
					}
					errored = errors.map(errorResponse => errorResponse.table)
					if (errored.length > 0) allFailedRetrievals.push(errored)
					retrieved = partitionedTables[promiseGroupIndex].filter(tableName => 
						!(errored && errored.length > 0 && errored.includes(tableName)))
					if (retrieved.length > 0) allSuccessfulRetrievals.push(retrieved)
					console.log("The following tables in group " + promiseGroupIndex + " were successfully retrieved: ", retrieved)
				}
			} catch(error) {
				// NOTE: this should never happen, as each retrieval does NOT throw exceptions
				console.log("Uncaught top level Error - terminating incremental script!: ", error)
				console.error(error, error.stack)
				continue
			}
		console.log("group retrieval iteration ends... should next retrieve group: ", promiseGroupIndex + 1)
		}
	}
	console.log("Incremental table subset retrieval has ended!")
	console.log("The following tables in this set were successfully retrieved: ", allSuccessfulRetrievals)
	console.log(allFailedRetrievals.length + " errors have occurred as follows: ", allFailedRetrievals )
}

/** Creates an ISO formatted version of the given date in string form, sanitized to avoid colons and periods
 * 
 * <p> Uses dashes ("-") instead of colons and periods so that the return value can be included as part of a 
 * filename.</p>
 */
const createTimestampString = (date) => {
	if (!date) {
		date = new Date()
	}
	//return date.toISOString().split('T')[0]
	return date.toISOString().replaceAll(":","-").replaceAll(".","-")
}

/** Ensures that a (local) directory exists (creates it when it does not)
 * 
 */
const ensureDirExists = (name) => {
	try {
		if (!Fs.existsSync(name)) {
			Fs.mkdirSync(name)
			console.log("Directory: " + name + " has just been created")
		} else {
			console.log("Directory: " + name + " already exists")
		}
	} catch(error) {
		console.log("Oops, could not ensure existence of directory: ", name)
		console.error(error, error.stack)
	}
}

/** Retrieves a list (array) of all the tables available in the database schema
 * 
 */
const retrieveTableListing = async (authResponse) =>  {
	// initialize query params
	if (authResponse) {
		authResponse = await ensureValidToken(authResponse)
	} else {
		authResponse = await ensureValidToken(currentlyValidToken)
	}
	if (authResponse) {
		let authToken = authResponse.access_token ? authResponse.access_token : authResponse
		let authHeaders = { "x-instauth": authToken } 
		console.log("About to query for table listing...")
		try {
			const response = await axios( { method: 'GET', url: tableListingEndpoint, headers: authHeaders})
			if (response && response.data) {
				console.log("Obtained response to table listing: ", response.data)
				//console.log("Will return: ", response.data.tables)
				return response.data.tables
			} else {
				console.log("Warning! cannot interpret the response to the listing request!", response)
			}
		} catch (error) {
			console.log("Obtained error from Axios when creating table listing job", error)
			console.error(error, error.stack)
			throw error
		}
	}
}



/** Retrieves (incrementally) all tables in the table schema
 * 
 */
const updateAllTables = async (folderName, updateTime) => {
	const authResponse = await obtainAuth(authEndpoint, authData)
	if (authResponse) {
		try {
			let allTables = await retrieveTableListing(authResponse)
			if (allTables) {
				return await retrieveIncrementalTableSubset(allTables, folderName, updateTime)
			}
		} catch (error) {
			console.error(error, error.stack)
			console.log("Error: Uncaught exception at the top script level - terminating full table update script", error)
		}
	}
}

/** Parses a filename of the form <table-name>_v<version>_<iso-UTC-timestamp>_part-<number>-<hash>.json.gz
 * 
 */
const parseFileName = (filename)=> {
	if (filename.indexOf("_20") > 0 && 
				(filename.indexOf(".json") > 0 || filename.indexOf(".json.gz" > 0))) {
		let versionstart = filename.indexOf("_v")
		let timestart = filename.indexOf("_20")
		let table = versionstart < 0 ? filename.substring(0, timestart) : filename.substring(0, versionstart) 
		let parts = filename.substring(timestart, filename.length).split("_")
		//console.log("Parts are: ", parts)
		if (parts) {
			let result = { table: table, at: parts[1], partId: parts[2]}
			if (versionstart > 0) result["schema_version"] = filename.substring(versionstart + 2, timestart)
			return result
		}
	}
	console.log("Warning!: filename could NOT be parsed: ", filename)
	return undefined
}

/** Parses a folder name (representing retrieved CD2 data) into constituent parts - format: snapshot_<iso-timestamp>|incremental_<iso-timestamp>
 * 
 */
const parseFolderName = (foldername) => {
	if (foldername) {
		let parts = foldername.split("_")
		if (parts) return { mode: parts[0], timestamp: parts[1]}
	}
	console.log("Warning!: foldername could NOT be parsed!", foldername)
	return undefined
}

/** Comparison function used for sorting (lexicographic)
 * 
 */
const comparisonFunction = (x,y) => x < y ? -1 : (x > y ? 1 : 0)

/** Sorts an array after applying the 'key' function to its constituent elements, using a given sort direction
 * 
 * <p> Direction can be either "asc" or "desc"
 */
const sortArray = (array, key, direction) => {
			//console.log("type of key is: ", (typeof key))
			return array.sort(function(x, y) {
				return direction === "asc" ? comparisonFunction(key(x), key(y)) : 
						 (comparisonFunction(key(x), key(y)))*(-1)
				})
}

/** Lists (local) files in a given directory
 * 
 */
const listFilesInDir = (path) => {
	let files
	try {
		  files = Fs.readdirSync(path)
		  // files object contains all files names
		  return files
		} catch (err) {
		  console.log("Warning: could NOT list files in directory: ", path)
		  console.error(err, err.stack)
		}
}

/** Finds a file on disk corresponding to the most recent table update for the given table
 * 
 * <p> The last table update is sought for in 'incremental' and 'snapshot' folders that are siblings of the folder given 
 * excluding the folder given. </p> 
 */
const findLastTableUpdate = async (table, folderName)=> {
	// 1- Find snapshot AND increm folders that are direct children of the current folder 
	// (with the exception of 'excludeFolderName")
	// 2- in this folder, find the file/s that corresponds to the table given, and take the most recent one
	// 3- return filename just found.
	// 4- if no match for the file in a particular folder, switch to the next folder in the listing until 
	// a file matching the table is found, and return it.
	let parentFolderPath  = path.dirname(folderName)
	let folderNameProper = folderName.substr(parentFolderPath.length)
	console.log("Will be looking for the last table update to " + table + " underneath folder: ", parentFolderPath)
	let  dirEntries = await fs.readdir(parentFolderPath, { withFileTypes: true });
	let onlyRelevantDirs = dirEntries.filter(de => de.isDirectory() && de.name !== folderName && 
			(de.name.indexOf("snapshot_") == 0 || de.name.indexOf("incremental_") == 0)) // 1/10/23 updated - check it out!!!
	//console.log("There are " + onlyRelevantDirs.length + " relevant directories I'll explore: ", onlyRelevantDirs)
	console.log("There are " + onlyRelevantDirs.length + " relevant directories I'll explore... ")
	let tableNameLength = table ? table.length : 0
	// 2- Order these folders starting from the most recent one (use the timestamps included as part of the folder names)
	// 1/10/23 onlyRelevantDirs = sortArray(onlyRelevantDirs, "name", "desc")
	onlyRelevantDirs = sortArray(onlyRelevantDirs, item =>{ return parseFolderName(item.name).timestamp}, "desc")
	let currentFolderIndex = 0
	let done = false
	let result
	if (onlyRelevantDirs && onlyRelevantDirs.length > 0) {
		while (!done) {// iteratively look for a file in the folders in order
			let currentFolder = onlyRelevantDirs.length > 0 ? onlyRelevantDirs[currentFolderIndex] : undefined
			if (currentFolder) {
				// 1/10/23 let currentCandidateFiles= listFilesInDir("./" + currentFolder.name)
				let currentCandidateFiles= listFilesInDir(parentFolderPath + "/" + currentFolder.name)
				currentCandidateFiles = currentCandidateFiles.filter(filename => filename.length > tableNameLength && 
						filename.substring(0, tableNameLength) === table) // only files matching the table remain
				// order the candidate files with most recent on top
				currentCandidateFiles = currentCandidateFiles.sort (function (x, y) {
					return comparisonFunction(x, y)*(-1)})
				if (currentCandidateFiles && currentCandidateFiles.length > 0) {
					result = currentCandidateFiles[0]
					done = true
				} else {// no candidate file in this folder -->  use next folder
					currentFolderIndex++
					if (currentFolderIndex >= onlyRelevantDirs.length) {
						done = true
					}
				}
			} else {// no existing file was found for the given table
				done = true
			}
		}
		console.log("The last table update for table: " + table + " was: ", result)
		return result
	}
}

/* Create a folder for the script's output and run it 
 * 
 */
const folderName = defaultTopFolder + "/" + "incremental_" + createTimestampString()
ensureDirExists(folderName)
updateAllTables(folderName)

