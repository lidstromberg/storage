# Storage - read/write utility for Google Cloud Platform Storage buckets.

A Go utility to facilitate reading/writing files from/to Google Cloud Platform Storage.

## What?
This utility uses [Google Application Credentials] or [GCP] service accounts to connect to a Google Cloud Platform project and read/write files from/to Storage buckets.

## Why?
This was written to provide [GCP] storage bucket functionality within [auth]. It wraps the Google provided [Storage Go client].

## How?
The best place to start is with the tests. If running locally, then ensure that  [Google Application Credentials] have been created. If running from a [GCP] virtual machine, then ensure that the relevant service account (compute, appengine etc.) has the following IAM scopes: 'Storage Object Viewer' and 'Storage Object Creator', or 'Storage Object Admin'. See [GCP service accounts] for further details.

## Examples
See the tests for usage examples.

## Dependencies and services
This utilises the following fine pieces of work:
* [GCP]'s [Datastore Go client] and [Storage Go client]

## Installation
Install with
```sh
$ go get -u github.com/lidstromberg/storage
```
#### Environment Variables
You will also need to export (linux/macOS) or create (Windows) some environment variables.

Change LB_DEBUGON to true/false if you want verbose logging on/off. The other variables don't need to be changed.
```sh
################################
# STORAGE
################################
export STOR_DEBUGON='true'
export STOR_CLIPOOL='5'

################################
# GCP CREDENTIALS
################################
export GOOGLE_APPLICATION_CREDENTIALS="/PATH/TO/GCPCREDENTIALS.JSON"
```
(See [Google Application Credentials])

### Main Files
| File            | Purpose       |
|-----------------|---------------|
| storage.go      | Logic manager |
| storage_test.go | Tests         |

### Ancillary Files
| File               | Purpose                                                  |
|--------------------|----------------------------------------------------------|
| config.go          | Boot package parameters, environment var collection      |
| const.go           | Package constants                                        |
| errors.go          | Package error definitions                                |
| env                | Package environment variables for local/dev installation |
| storagetester.json | test content file                                        |
| gogets             | Statements for go-getting required packages              |

   [auth]: <https://github.com/lidstromberg/auth>
   [GCP]: <https://cloud.google.com/>
   [Datastore Go client]: <https://cloud.google.com/datastore/docs/reference/libraries#client-libraries-install-go>
   [Storage Go client]: <https://cloud.google.com/storage/docs/reference/libraries#client-libraries-install-go>
   [Google Application Credentials]: <https://cloud.google.com/docs/authentication/production#auth-cloud-implicit-go>
   [GCP service accounts]: <https://cloud.google.com/iam/docs/understanding-service-accounts>