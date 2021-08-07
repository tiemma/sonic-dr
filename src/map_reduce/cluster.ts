import cluster, {ClusterSettings, Worker} from "cluster";
import {cpus} from "os"
import {pid} from "process";
import {Queue} from "@tiemma/sonic-core";
import {DBMetadataGraph, MapReduceEvent, Result} from "../types";
import {restore} from "../strategy";
import {DataTypes, Options, Sequelize, QueryTypes} from "sequelize"
import {readFileSync, writeFileSync} from "fs";
import config from "../../config.json";
import {getDBInstance} from "../models";

const numCPUs = cpus().length;

const clusterEvents = {
    ONLINE: "online",
    MESSAGE: "message",
    DISCONNECT: "disconnect"
}

const sequelize = new Sequelize({
    dialect: 'sqlite',
    storage: `${process.cwd()}/db`,
    benchmark: false,
    logging: false
});

const Tables = sequelize.define("tables", {
    name: {
        type: DataTypes.STRING,
        unique: true,
        primaryKey: true
    },
    isProcessed: {
        type: DataTypes.BOOLEAN
    }
}, {
    timestamps: false,
})

let loggerID = "UNSET"

const logger = (message: any, date = new Date().toISOString()) => console.log(`${date}: ${loggerID}: ${message}`)

const readBackup = (table: string) => readFileSync(`${process.cwd()}/backup/${table}.sql`).toString()

const Delay = (time = 5000) => new Promise(resolve => setTimeout(resolve, time))

const GetCount = (metadata: DBMetadataGraph, table: string) => {
    return Tables.sequelize.query(`SELECT COUNT(*) AS count FROM tables WHERE name IN (:names) AND isProcessed IS TRUE`,
        {
            replacements: {
                names: Array.from(metadata.tableDependencies[table])
            },
            plain: true
        }
    )
}

const ensureDependenciesSatisfied = async (metadata: DBMetadataGraph, table: string) => {
    const dependencySize = metadata.tableDependencies[table].size
    if (dependencySize > 0) {
        let filledTables = await GetCount(metadata, table)
        while (filledTables["count"] != dependencySize) {
            filledTables = await GetCount(metadata, table)
        }
        logger(`Dependencies fulfilled for table ${table}, expected=${dependencySize}, found=${filledTables["count"]}`)
    }
}

const getWorkerID = (workerQueue: Queue, workers: NodeJS.Dict<Worker>) => {
    let workerID = workerQueue.dequeue()
    while (!workerQueue.isEmpty() && !workers[workerID]) {
        workerID = workerQueue.dequeue()
    }

    return workerID
}

const Map = async (workers: NodeJS.Dict<Worker>, workerQueue: Queue, event: MapReduceEvent, metadata: DBMetadataGraph) => {
    const {table} = event;

    if (await IsProcessed(table)) {
        return
    }

    await ensureDependenciesSatisfied(metadata, table)

    const workerID = getWorkerID(workerQueue, workers)
    logger(`Sending event ${JSON.stringify(event)} to worker ${workerID}`)
    workers[workerID].send(event)
}

const IsProcessed = async (table: string) => {
    let count = await Tables.count({where: {name: table}})

    if (!count) {
        logger(`Unprocessed ${table} found`)
        await Tables.create({name: table, isProcessed: false});
        return false
    }

    return true
}

export const MapReduce = async (metadata: DBMetadataGraph, adjMatrix: Result, config: Options) => {
    writeFileSync(`${process.cwd()}/adjMatric.json`, JSON.stringify(adjMatrix, null, '\t'))

    const isMaster = cluster.isMaster || cluster.isPrimary;

    await sequelize.authenticate({benchmark: true})

    const processOrder = new Queue()

    if (isMaster) {
        const workerQueue = new Queue()
        await Tables.sync({force: true, logging: true})

        loggerID = "MASTER"

        logger("Running Map reduce");
        logger(`Process running on pid ${pid}`);

        (cluster.setupMaster || cluster.setupPrimary)({
            execArgv: ['-r', 'tsconfig-paths/register', '-r', 'ts-node/register', '--async-stack-traces']
        } as ClusterSettings)

        for (let i = 0; i < numCPUs; i++) {
            const worker = cluster.fork()

            worker.on(clusterEvents.ONLINE, () => {
                workerQueue.enqueue(worker.id)
            })

            worker.on(clusterEvents.MESSAGE, (message: Record<string, number | string>) => {
                if (message["table"]) {
                    processOrder.enqueue(message["table"])
                }
                logger(`Worker ${message.id} now available`)
                workerQueue.enqueue(message.id)
            })

            worker.on(clusterEvents.DISCONNECT, () => {
                logger(`Gracefully shutting down worker #${worker.id}`)
            })
        }

        logger("Worker queues initializing")
        while (workerQueue.getElements().length != numCPUs) {
            await Delay(10000)
        }
        logger(`Workers queue populated`)

        for (const [root, dependencies] of Object.entries(adjMatrix)) {
            let max = 0
            for (const tableSet of dependencies) {
                max = Math.max(max, tableSet.length)
            }

            for (let i = 0; i < max; i++) {
                for (let j = 0; j < dependencies.length; j++) {
                    if (i < dependencies[j].length) {
                        await Map(cluster.workers, workerQueue, {table: dependencies[j][i]}, metadata)
                    }
                }
            }
            Map(cluster.workers, workerQueue, {table: root}, metadata)
        }

        logger(processOrder.getElements())

        for (let i = 0; i < numCPUs; i++) {
            if (cluster.workers[i]) {
                await Delay(100)
                cluster.workers[i].disconnect()
            }
        }

        logger(`Shutting down master`)

        process.exit(0)
    } else if (cluster.isWorker) {
        loggerID = `WORKER-${cluster.worker.id}`

        process.on(clusterEvents.MESSAGE, (event: MapReduceEvent) => {
            const {table} = event

            logger(`Processing table ${table}`)

            const dbInstance = getDBInstance(config)
            dbInstance.sequelize.query(`DROP TABLE IF EXISTS "${table}" CASCADE;\n` + readBackup(table), {
                type: QueryTypes.INSERT,
                benchmark: false,
                logging: false
            }).then(() => {
                Tables.update({isProcessed: true}, {where: {name: table, isProcessed: false}})
                    .then((res) => {
                        const data = {id: cluster.worker.id}

                        if (res[0]) {
                            data["table"] = table
                            logger(`Processed table ${table}`)
                        }

                        process.send(data)
                    })
                dbInstance.sequelize.close()
            }).catch((err) => {
                logger(`Error: ${err.parent.detail}`)
                dbInstance.sequelize.close()
            })
        })
    }
}

(async () => {
    const {metadata, results} = restore()
    await MapReduce(metadata, results, config as Options)
})();
