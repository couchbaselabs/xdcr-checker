xdcr-checker
============

** Requires Couchbase Server version 2.x **

This tool checks that all documents and mutations are replicated to the destination cluster. It can be used to augment the built-in XDCR monitoring capability included in Couchbase Server and complements exisitng standard network monitoring within a production environement.
It works by receiving a stream of all documents in a SOURCE cluster and any subsequent mutations. For each of these document keys that are recieved on this stream, it checks that the destination cluster:
1. Also contains the corresponding key
2. That key has the same CAS value as the destination cluster.

Care must be taken to avoid "false positives" - there are some legitimate reasons why the check may fail - primarily if a given mutation is superseded by a further mutation on the source cluster.

## Configuration ##
Setting up the source and destination cluster credentials is performed in XDCRChecker.java.
This file also contains further tunables such as the number of times to retry and the delay time in between checks.

## Usage ##
Projected can be imported into an IDE (e.g. Eclipse) as a maven project. Alternatively can be invoked from the command line as follows:

    export CLASSPATH=$CLASSPATH:<PATH TO COUCHBASE JAVA SDK JARS>:<PATH TO xdcr-checker JARs>
    javac XDCRChecker.java MutationChecker.java
    java  XDCRChecker
  
## Performance ##
There will be an initial burst of checks whilst all existing documents are checked. The ongoing number of operations will be proportionate to the number of Create, Update and Deletion operations occurring on the source cluster.
