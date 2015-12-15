#Parsek 

[![Build Status](https://travis-ci.org/andr83/parsek.svg)](https://travis-ci.org/andr83/parsek)

Parsek designed for parse, validate and transform log files in different formats. It can be used as a library or standalone [Apache Spark](https://spark.apache.org) application.

##Overview

![Parsek workflow](https://drive.google.com/thumbnail?id=0B6RjvrMY5aLaU0NYVEYwVWF4Rzg&authuser=0&v=1450197607380&sz=w2480-h1280)

Parsek allow organise work process in pipes. Where each pipe is a unit of work and multiple pipes can be join in pipeline. 

In Parsek data internally presented as JSON like [AST](https://github.com/andr83/parsek/blob/master/core/src/main/scala/com/github/andr83/parsek/PValue.scala). On every step pipe accept PValue and must transform it to other PValue.

Example of pipes: parseJson, parseCsv,  flatten, merge, validate and etc. 

Source can read data from different source type and convert to Parsek AST.  Currently supported sources:

 - Local text files
 - Hadoop text/sequence* files
 - Kafka stream*

> marked with * not implemented yet

Sink allow to output data in AST format to external sources. Supported sinks:

- Local text files with csv/json serialization.
- Hadoop files with csv/json/avro* serialization.

> marked with * not implemented yet

##Spark application usage

To run assembly jar just type:

    java -jar parsek-assembly-xx-SNAPSHOT.jar --config /path/to/config_file.conf

Parsek spark application use configuration file to define job task.  More about config format [read here](https://github.com/typesafehub/config).

Example of configuration file:
```yaml
sources: [{
	type: textFile
	path: "events.log"
}]

pipes: [
{
	type: parseRegex
	pattern: ".*\\[(?<body>[\\w\\d-_=]+)\\].*"
},{
    type: parseJson
	field: body
},{
    type: validate
	fields: [{
				type: Date
				name: time
				format: "dd-MMM-yyyy HH:mm:ss Z"
				toTimeZone: UTC
			},{
				type: String
				name: ip
				pattern: ${patterns.ip}
			},{
				type: Record
				name: body
				fields: [{
					type: Date
					format: timestamp
					name: timestamp
					isRequired: true
				},{
					type: List
					name: events
					field: {
						type: Map
						name: event
						field: [{
							type: String
							name: name
							as: event_name
						}]
					}
				}]
			}]
},{
	type: flatten
	field: body.events
}
]

sinks: [{
	type: textFile
	path: /output
	serializer: {
		type: csv
		fields: [time,ip,timestamp,event_name]
	}
}]
```

In this example configuration file we define:

1. Read lines from `events.log` file
2. Parse each line with regular expression and extract field `body`
3. Parse `body` field as json
4. Validate json value
5. Flatten embeded list in `body.events` field
6. Save result as csv to `/output` directory.
