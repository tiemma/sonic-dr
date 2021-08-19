# sonic < dr >

![image](https://raw.githubusercontent.com/Tiemma/sonic-core/master/image.png) 

Accelerate your Disaster Recovery processes on relational databases (table backup and restores)

![CodeQL](https://github.com/Tiemma/sonic-dr/workflows/CodeQL/badge.svg)
![Node.js CI](https://github.com/Tiemma/sonic-dr/workflows/Node.js%20CI/badge.svg)

# What does this do?

Distributed backup and restore actions towards disaster recovery for large and mid-scale databases.

Adds in flexibility for configuring what parts of your data to backup and speeds up the grossly linear process with most dump utilities (pgdump, mysqldump etc).

# How to use it

- Install the package
- Configure the args
- Backup and restore your tables as needed

## Install the package

To install the package, run
```bash
npm install --save @tiemma/sonic-dr
```

## Configure the args

`sonic < dr >` utilises `sonic < distribute >` under the hood for distributed processing parts and sequelize for database connections.

```typescript
export interface DRArgs {
  numWorkers: number; // number of workers to use 
  config: Options;    // sequelize compatible configuration for database connection
  tableSuffixes: Record<string, string>; // key value string containing extra queries to apply when selecting data across tables
}
```

`tableSuffixes` adds a new touch to DR backups allowing you to specify alternate suffixes to queries so only some data is written out of the table.
This is useful for multi-tenant systems seeking ways to extract tenant specific data at scale.

e.g if I have a table called users and I wish to select a user with id 1, my config is as such
```typescript
const drArgs = {
    numWorkers: a number,
    config: {
        //...some database configuration
    },
    tableSuffixes: {
        "user": "WHERE id = 1"
    }
}
```

This adds some ease to the query process and allows for more streamlined logical backup operations.


## Backup and restore your tables as needed
After configurating the args, you can call the sonicDR method and backup and restore your tables as needed.

```typescript
const drArgs = {
    numWorkers: 2,
    config: {
        //...some database configuration
    },
    tableSuffixes: {
        // all base tables are referenced by t
        "user": "WHERE t.id = 1",

        // INNER JOINS also work but only selections on the base table would be picked out e.g t.*
        "cluster": "INNER JOIN cluster_user cu ON cu.cluster_id = t.id WHERE t.id = 1" 
    }
};

sonicDR("backup" | "restore", drArgs);
```

This example would spin up 2 worker processes, backup all the data in all the tables using the queries where sufficient for the tables described in `tableSuffixes`.

All this information would be available in the `backup/files` folder.


# Why did I do this?

In the past, dump tools have been effective, but they lack the concurrency, and some applicable ease to manage large databases and tenant-based logical backups.

They also do not have foreign key ordering, and restore and backup processes being linear do not help with speed in their operations.

This package not only solves that but uses a novel method of converting the foreign key relationships into dependency graphs for topological sorting during backups and matrix-based dependency resolution methods for concurrent-safe restores.


# Notes and specs on the libraries strategy

> Code for this can be referred from the [src/strategy](./src/strategy) directory.

## Backup

The backup process involves two parts:

1. Generate a dependency graph and in-degree map based on foreign key relationships

This is accomplished by using carefully crafted queries and JSON aggregations to obtain information about tables and their respective foreign key relationships with other tables.

```
{
	"tableDependencies": {
		"A": [
			"C" // A depends on C existing first
		]
    }, 
    "inDegreeMap": {
     		"A": [
     			"B"  // B depends on A existing first
     		]
    }
}
```

An in-degree map representing a table, and the tables that depend on it is generated at this time to assist with the restore process along a later line.

This aids not only in viewing the DB relationships, but the backups can be broken down in tabular parts and assembled later on by the restore process. 


2. Backup files in order and write data obtained from 1 into a metadata.json file in the root backup directory

After the generation of data from 1, the map is sorted to ensure it is resolvable for integrity reasons and is written out by a normal backup, processed across mapped workers in a concurrent fashion.


## Restore

1. Generate a dependency matrix to assist with resolving multiple table foreign key relationships

This is generated using the inDegree map and sets a route from the independent table to the table in question with a dependency.

The multiple rows allow us to resolve multiple foreign keys and hence the tables independently across various workers.

```text
{
	"A": [
		["B","C","D"], // B is written so C can be imported fine so D can be imported fine
		["B","C","E"]  // B has no dependencies, C depdends on B and E depends on C 
	]
}
```

Like getting the height of a  tree, we process column wise with deduplication to achieve the following

```text
       D   \
B - C /     A
      \    /
       E  /
```                          

This example as shown, allows us to process D and E concurrently. 

As a table gains more foreign key relationships, we also then have a bigger number of concurrent table imports as opposed to linear reads on a basic sql restore process.

Integrity and locking is done using shm via sqlite to ensure dependencies are resolved before moving to dependent stages.

This process is then concurrently ran per table per dependency until all the tables are resolved.
                                                  
                          
# Best Practices

## Blobs and non-text data might not be best in a DB

This tool is built to aid faster logical DR operations for text based information on large DBs.

Blobs and other non-textual data types are not handled, although they would be written out in utf-8 encoding, of which I cannot state the efficacy of this.

I do not know why this would be a thing but do consider dropping these tables (specifically the columns), and  I mean PERMANENTLY 🙂.


# Future Plans

## Stable API access for schema exports 

At the moment, there are workarounds for obtaining the table schemas which means that I directly execute the dump utilities per database type (mysqldump, pgdump etc) to obtain the schema information for tables.

This is more of a hack in clear notes.

I have experimented with generating the schema from `information_schema` data as available but this is rather tedious as of now.

If you have an idea of how to do this or wish to contribute this, Open up a PR using the ISSUE TEMPLATE [here](./.github/ISSUE_TEMPLATE/feature_request.md)


## Failure handling and backup tests for integrity

As of now, the data is written out but there's no way to test if something went wrong during a backup e.g missing quotes, poorly stringify-ed objects e.g JSON columns, arrays etc

So there's a need for some method to ensure the data passed out can be written back.

```text
This is something frustrating with using the normal dump and restore tools for databases

Anyone who's administered a DB backup knows just how funny a backup made at an instant can't be restored due to weird SQL format reasons
```

If you have an idea of how to do this or wish to contribute this, Open up a PR using the ISSUE TEMPLATE [here](./.github/ISSUE_TEMPLATE/feature_request.md)


## More model support which translates to more relational DB support coverage

I currently extend support for different relational databases (currently supports MySQL and Postgres) via models which implement the queries to generate the graphs and matrixes for operations which are DB-dependent.

If you can implement the APIs already defined by the [AbstractModel](./src/models/AbstractModel.ts), Open up a PR using the ISSUE TEMPLATE [here](./.github/ISSUE_TEMPLATE/feature_request.md)


# Debugging

By default, logs are shown.

If you prefer no logs, kindly set the QUIET env variable.

```bash
export QUIET=true
```

# I found a bug, how can I contribute?
Open up a PR using the ISSUE TEMPLATE [here](./.github/ISSUE_TEMPLATE/feature_request.md)
