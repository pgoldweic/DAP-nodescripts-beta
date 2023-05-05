require('dotenv').config();

const base_URL = process.env.dap_URL
const dap_URL = base_URL + "/dap"

const axios = require('axios').default;

const sleepMs = process.env.sleepMilliseconds || 10000

const Fs = require('fs')
const path = require('path')
const Https = require('https')

const jwts = require('jsonwebtoken')

const querystring = require('querystring')

// maximum number of simultaneous queries to be sent to the DAP service
const maxSimultaneousQueries = process.env.dap_maxSimultQueries || 10

// Parameters used in an auth request 
// 12/7/22 Newly defines the body of the auth request
const authData = { grant_type:'client_credentials'}

// Authorization endpoint
// 12/20/22 const authEndpoint = dap_URL + "/auth" 
const authEndpoint = base_URL + "/ids/auth/login"

// Job monitoring endpoint
const pollJobEndpointBase  = dap_URL + "/job/"  

// Table listing endpoint
const tableListingEndpoint = dap_URL + "/query/canvas/table"

// will hold the currently valid auth token
var currentlyValidToken
var currentlyValidTokenResponse 

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

/** Returns job information for a newly started table retrieval job
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
		// 12/20/22 let authHeaders = { "Authorization": "Bearer " + authToken } 
		let authHeaders = { "x-instauth": authToken } 
		let queryParams = { "format": format || 'jsonl' }
		if (since) queryParams["since"]= since
		if (until) queryParams["until"]= until
		if (filter) queryParams["filter"]= filter
		let queryTableEndpoint = dap_URL + "/query/canvas/table/" + table + "/data" 
		console.log("Table to be queried: " + table + " using settings: ", queryParams)
		try {
			const response = await axios( { method: 'POST', url: queryTableEndpoint, data: queryParams, headers: authHeaders})
			if (response && response.data) {
				console.log("Job " + response.data.id + " was successfully created for retrieval of " + table + " and has " + 
						(response.data.objects ? response.data.objects.length : 'NO') + " objects associated to it")
				return response.data
			}
		} catch (error) {
			console.log("Obtained error from Axios when creating retrieval job for table: " + table, error)
			console.error(error, error.stack)
			throw error
		}
	}
}

/** Monitors a (table retrieval) job which has already started running
 * 
 */
const monitorJob = async (jobId, authResponse) => {
	let jwt
	let authHeaders
	let pollJobEndpoint = pollJobEndpointBase + jobId
	//let authHeaders = { "Authorization": "Bearer " + jwt } 
	//console.log("Monitoring job: ", jobId)
	let jobStatus = "running"
	let result = undefined
	let suspend = false
	while (jobStatus === "running" || jobStatus === "waiting") {
		//console.log("Beginning of monitoring iteration for job: ", jobId)
		if (authResponse ) {
			authResponse = await ensureValidToken(authResponse)
		} else {
			authResponse = await ensureValidToken(currentlyValidToken)
		}
		
		if (authResponse ) {
			// await delay(sleepSeconds)
			// console.log("Polling job: ", jobId)
			jwt = authResponse.access_token ? authResponse.access_token : authResponse
			//12/20/22 authHeaders = { "Authorization": "Bearer " + jwt } 
			authHeaders = { "x-instauth": jwt } 
			try {
				let response = await axios( { method: 'GET', url: pollJobEndpoint, 
					headers: authHeaders})
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
		//console.log("End of monitoring iteration for job: ", jobId)
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
		//let authHeaders = { "Authorization": "Bearer " + jwt } 
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
 *		  'part-0000-blah.json.gz': {
 *			    url: 'https://data-access-platform-output-prod-iad.s3.amazonaws.com/output/rootAccountId%3DWTbP67mC863Zx9qZ1XtqGSxhiLCO5sjJQ2lPGfgf/queryId%3D5293121f-6250-4aa4-b050-2e4c5e5ac645/part-0000-blah.json.gz?X-Amz-Security-Token=FwoGZXIvYXdzEHwaDIxZDeQmXcZnSr57rCK7Ab8UcDOkeegNuTuA%2B0xHHddbR1%2Bmcy2prq2MIMigBV3ItGLmnpRBxb0i%2B%2FW37WJjW%2FhRUcj9FzGEHGlzhm2TnHE41TYRWelAHAsNtBNqPDUIaZlyxOU6jBmihEaHbS6O0PxNunNTFrs1UI3gRgekvkpvZOnBlXmzd1eENNUWyKtYOLPm0kChPY0h73UYcuyn4O0cR27SopjIoYnX0bWxJGYxdOZ70f%2BZ3yg9VQ9QsViqsZ7qZijpw58znysoirPemAYyLb10UK4BqtD5LJzeaexFf%2BPsfmpW6WsToy%2BkSdajKw6jD0DxoO2xxa3apfbDmg%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220906T190646Z&X-Amz-SignedHeaders=host&X-Amz-Expires=3600&X-Amz-Credential=ASIAXX2PINZLDE5NNTHX%2F20220906%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=7497dc9941bf5ae73cf73c8b3def019e15f55973bb658f2ca4fcb41efa950cda'
 *			  }
 *			}
 *	<p> Filenames are created as follows:
 *		alternative 1 (when process.env.includeSchemaVersionInFilenames is false): 
 *		- <tableName>_<atTimestamp>_<filenameTokenFromUrl>  	OR
 *		alternative 2: (when process.env.includeSchemaVersionInFilenames is true):
 *		- <tableName>_v<versionNumber>_<atTimestamp>_<filenameTokenFromUrl>
 */
const downloadAllData = async (urls, table, at, folderName, schema_version) => {
	// Ensure folder exists
	ensureDirExists(folderName)
	// Download Data to the specified folder
	at = at.replaceAll(':','-')
	console.log("Will download table data retrieved as of: ", at + " for table: " + table + " into folder: "+ folderName + " for schema version: ", schema_version)
	let allPromises = []
	let pathname
	for (let objectId in urls) {
		try {
			if (urls.hasOwnProperty( objectId)) {
				let urlObject = urls[objectId]
				let fileUrl = urlObject.url
				let fileNameTokens = fileUrl.split("/")
				//let fileName = table + "_" + at + "_" // TODO: add schema version to filename like <table_name>_v<version>_<at>_
				let fileName = table + 
					(process.env.includeSchemaVersionInFilenames && 
						process.env.includeSchemaVersionInFilenames === "true" ? ("_v" + schema_version ) : "") + 
							"_" + at + "_" // 12/20/22 now recording schema_version depending on configuration parameter
				if (fileNameTokens && fileNameTokens.length) {
					fileName += fileNameTokens[fileNameTokens.length -1]
					let params = fileName.indexOf("?")
					if (params >=0) fileName = fileName.substring(0, params)
					
				}
				if (fileName) {
					pathname = folderName ? folderName + "/" + fileName : fileName
					//console.log("About to download data from url: " + fileUrl + " onto: " + fileName)
					console.log("Downloading data from url: " + fileUrl + " onto: " + pathname + " for table: " + table + " and schema version: ", schema_version)
					//allPromises = allPromises.concat([downloadFile(fileUrl, fileName)])
					allPromises = allPromises.concat([downloadFile(fileUrl, pathname)])
				}
			}
		} catch (error) {
			console.log("Error creating download promise for object: " + objectId, error)
			console.error(error)
		}
	}
	console.log("Prepared " + allPromises.length + " download promises for table: ", table)
	try {
		return await Promise.all(allPromises)
	} catch (error) {
		console.log("Error downloading file/s!", error)
		console.error(error, error.stack)
	}
}

/** Downloads a given (table data) URL onto a local file
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

/** Returns a promise which resolves in 'time' milliseconds
 * 
 * @param time
 * @returns
 */
function delay(time) {
	  return new Promise(resolve => setTimeout(() => resolve(true), time));
} 

/** Fully retrieves a given table and downloads it onto one or more files on the local disk
 * 
 */
const retrieveCompleteTable = async(table, folderName) => {
	console.log("Getting started with retrieval of data for table: " + table + " into folder: " + folderName)		
	let monitoringData
	let result
	try {
		let authResponse = currentlyValidToken ? 
					await ensureValidToken(currentlyValidToken) : 
					await obtainAuth(authEndpoint, authData)
		if (authResponse) {
			let job = await retrieveTable (table, "jsonl", authResponse, undefined, undefined, undefined)
			if (job) {
				const {id, status, at, schema_version, objects} = job
				if (status === "failed") {
					throw new Error("Retrieval job terminated with a 'failed' status for table: " + table)
				}
				if (status === "complete") {
					//console.log("Great! Retrieval job: " + id + " completed successfully at (recorded) time", at)
					console.log("Great! Retrieval job: " + id + " completed successfully at (recorded) time" + at + 
									" and has " + (objects ? objects.length : 'NO') + " objects associated to it!")
					let urlsRetrieved = await retrieveObjectURLs(job)
					if (urlsRetrieved) {// retrieve each object via their URL
						console.log("Will now retrieve the following object URLs:", urlsRetrieved)
						result = await downloadAllData(urlsRetrieved, table, at, folderName, schema_version)
						if (result) {
							console.log("Yay! I downloaded all the data for table: " + table + " by time: " + new Date())
						}
						return result
					}
				} else {// status is running or waiting
					if (id && (status === "running" || status === "waiting")) {// success ==> need to wait until retrieval job completion
						console.log("Starting to monitor job: ", id)
						// 10/26/22 monitoringData = await monitorJob (id, authResponse)
						monitoringData = await monitorJob (id)
						if (monitoringData) {// job completed 
							const {id, status, at, schema_version, objects} = monitoringData
							if (status === "complete") {
								console.log("Great! Retrieval job: " + id + " completed successfully and is current as of " + at + 
									" and has " + (objects ? objects.length : 'NO') + " objects associated to it!")
								// 10/26/22 let urlsRetrieved = await retrieveObjectURLs(monitoringData, authResponse)
								let urlsRetrieved = await retrieveObjectURLs(monitoringData)
								if (urlsRetrieved) {// retrieve each object via their URL
									console.log("Will now retrieve the following object URLs:", urlsRetrieved)
									result = await downloadAllData(urlsRetrieved, table, at, folderName, schema_version)
									if (result) {
										console.log("Yay! I downloaded all the data for table: " + table + " by time: " + new Date())
									}
								}
							} else if (status === "failed") {// job failed while monitoring  it
								console.log("Oops! Retrieval job: " + id + " failed to complete!")
								throw new Error("Table: " + table + " could NOT be retrieved since retrieval job: " + id + " failed!")
								
							}
						} 
						return result
					}
				}	
			}
		}
	} catch (error) {
		console.log("Catching uncaught exception within retrieveCompleteTable! - unable to retrieve: " + table + " at time: " + new Date())
		console.error(error, error.stack)
		return {error: error, table: table}
	}
}

/** Retrieves the table listing (temporarily) from a local schema file (provided by Instructure: schema.json)
 *  NOTE: not used anymore
 */
const retrieveTablesSchema = () => {
	const schema = require('./schema.json')
	return schema
}

/** Retrieves a subset of tables given by an array of table names and a folder path
 * 
 */
const retrieveTableSubset = async(tablesList, folderName)=> {
	console.log("Will try to retrieve the following table subset... ", tablesList)
	let table
	let errored
	let retrieved
	let allSuccessfulRetrievals = []
	let allFailedRetrievals = []
	let allErrors = []
	let partitionedTables = partitionArrayIntoGroups(tablesList, maxSimultaneousQueries)
	//let partitionedPromises = partitionArrayIntoGroups(promises, maxSimultaneousQueries)
	console.log("A table subset retrieval was partitioned into " + partitionedTables.length + " groups")
	// Submit each of the partitioned promise groups in sequence
	// let promiseGroupIndex = 0
	// let done = false
	if (partitionedTables.length > 0) {
		//while (!done) { 
		for (let promiseGroupIndex = 0; promiseGroupIndex < partitionedTables.length; promiseGroupIndex++) {
			// submit in sequence all promise subgroups
			try {
				console.log("Partition retrieval iteration now starts for group: ", promiseGroupIndex)
				//console.log("Will try to retrieve all tables in partition group: " + promiseGroupIndex)
				console.log("Tables in this subgroup are: ", partitionedTables[promiseGroupIndex])
				let responses = await Promise.all(partitionedTables[promiseGroupIndex].map(table => retrieveCompleteTable(table, folderName)))
				while (!responses) {
					await delay(sleepMs)
				} 
				if (responses) {
					console.log("Finished retrieving all the tables in group: ", promiseGroupIndex)
					let errors = responses.filter(response => response && response.error && response.table)
					if (errors && errors.length > 0) {
						console.log(errors.length + " errors have occurred as follows: ", errors )
						allErrors.push(errors)
					} else {
						console.log("Yay! all subgroup retrievals were successful in group: ", promiseGroupIndex)
					}
					errored = errors.map(errorResponse => errorResponse.table)
					if (errored.length > 0) allFailedRetrievals.push(errored)
					retrieved = partitionedTables[promiseGroupIndex].filter(tableName => 
						!(errored && errored.length > 0 && errored.includes(tableName)))
					console.log("The following tables in group " + promiseGroupIndex + " were successfully retrieved: ", retrieved)
					if (retrieved.length > 0) allSuccessfulRetrievals.push(retrieved)
					
				} 
			} catch(error) {
				console.log("Uncaught top level Error - terminating script!: ", error)
				console.error(error, error.stack)
				/* promiseGroupIndex++
					if (promiseGroupIndex === partitionedPromises.length) {// we are done
						done = true
					}
				*/
				continue
			}
		console.log("Partition retrieval iteration ends... should next retrieve group: ", promiseGroupIndex + 1)
		}
	}
	/* old code does all of them simultaneously
	try {
		let responses = await Promise.all(promises)
		if (responses) {
			console.log("Looks like I've finished retrieving all the tables in the input set!")
			let errors = responses.filter(response => response && response.error && response.table)
			if (errors && errors.length > 0) {
				console.log(errors.length + " errors have occurred as follows: ", errors )
			} else {
				console.log("Yay! all retrievals were successful!")
			}
			errored = errors.map(errorResponse => errorResponse.table)
			retrieved = tablesList.filter(tableName => 
				!(errored && errored.length > 0 && errored.includes(tableName)))
			console.log("The following tables were successfully retrieved: ", retrieved)
		}
	} catch(error) {
		console.log("Uncaught top level Error - terminating script!: ", error)
		console.error(error, error.stack)
	}
	*/
	console.log("Snapshot table subset retrieval has ended!")
	console.log("The following tables in this set were successfully retrieved: ", allSuccessfulRetrievals)
	console.log(allFailedRetrievals.length + " errors have occurred as follows: ", allFailedRetrievals )
	
}

/** Partitions an array into a set of sub-arrays with at most 'maxElements' elements each
 * 
 */
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

/** Creates a (sanitized version) of the ISO formatted string counterpart given a date
 * 
 * <p> The ISO date is 'sanitized' by replacing colons and dot characters with dashes, so 
 * that the resulting value can be be used within a folder's name in the local file system. </p>
 */
const createTimestampString = (date) => {
	if (!date) {
		date = new Date()
	}
	return date.toISOString().replaceAll(":","-").replaceAll(".","-")
}

/** Ensures that a (local) directory exists and creates one when it does not
 * 
 */
const ensureDirExists = (name) => {
	try {
		if (!Fs.existsSync(name)) {
			Fs.mkdirSync(name)
			console.log("Directory: " + name + " has just been created")
		} else {
			//console.log("Directory: " + name + " already exists")
		}
	} catch(error) {
		console.log("Oops, could not ensure existence of directory: ", name)
		console.error(error, error.stack)
	}
}

/** Retrieves a list (array) of all the tables available in the database
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
		// 12/20/22 let authHeaders = { "Authorization": "Bearer " + authToken }
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

/** Retrieves the database schema for a particular table (NOTE: NOT USED - present just in case)
 * 
 */
const retrieveTableSchema = async (table, authResponse) => {
	let tableSchemaEndpoint = dap_URL + "/query/canvas/table/" + table + "/schema"
	if (authResponse) {
		authResponse = await ensureValidToken(authResponse)
	} else {
		authResponse = await ensureValidToken(currentlyValidToken)
	}
	if (authResponse) {
		let authToken = authResponse.access_token ? authResponse.access_token : authResponse
		// 12/20/22 let authHeaders = { "Authorization": "Bearer " + authToken }
		let authHeaders = { "x-instauth": authToken } 
		console.log("About to query for table schema for table: " , table)
		try {
			const response = await axios( { method: 'GET', url: tableSchemaEndpoint, headers: authHeaders})
			if (response && response.data) {
				console.log("Obtained response to table schema: ", response.data)
				console.log("Will return: ", response.data.schema)
				return response.data.schema
			} else {
				console.log("Warning! cannot interpret the response to the table schema request for table: " + table + "!", response)
			}
		} catch (error) {
			console.log("Obtained error from Axios when creating table listing job", error)
			console.error(error, error.stack)
			throw error
		}
	}
}

/** Retrieves all tables in the table schema read from a local file  (NOT USED ANYMORE)
 * 
 */
const retrieveAllTables = async (folderName) => {
	const authResponse = await obtainAuth(authEndpoint, authData)
	if (authResponse) {
		try {
			let allTables = await retrieveTableListing(authResponse)
			if (allTables) {
				return await retrieveTableSubset(allTables, folderName)
			}
		} catch (error) {
			console.error(error, error.stack)
			console.log("Error: Uncaught exception at the top script level - terminating full table retrieval script", error)
		}
	}
	// 11/18/22 return await retrieveTableSubset(allTables, folderName)
}

/* Creates a folder for the script's output and runs it 
 * 
 */

const folderName = defaultTopFolder + "/" + "snapshot_" + createTimestampString()
ensureDirExists(folderName)
// Here's an example of how to retrieve a table subset
// retrieveTableSubset( ['accounts', 'wiki_pages'], folderName)

// Use this to retrieve all tables
retrieveAllTables(folderName)

