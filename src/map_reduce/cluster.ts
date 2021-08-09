import cluster from "cluster";
import {pid} from "process";
import {Queue} from "@tiemma/sonic-core";
import {DBMetadataGraph, MapReduceEvent, Result} from "../types";
import {restore} from "../strategy";
import {Options, QueryTypes} from "sequelize"
import config from "../../config.json";
import {getDBInstance} from "../models";
import {
    clusterEvents,
    ensureDependenciesSatisfied,
    getWorkerID,
    NUM_CPUS,
    Tables,
    getLoggerID,
    isMaster,
    isProcessed,
    getMaxDependencyCount, initMaster, shutdown
} from "./utils";
import {Delay, getLogger, readBackup} from "../utils";


const Map = async (workerQueue: Queue, event: MapReduceEvent) => {
    if (await isProcessed(event.table)) {
        return
    }

    const workerID = await getWorkerID(workerQueue)

    getLogger(getLoggerID())(`Sending event ${JSON.stringify(event)} to worker ${workerID}`)

    cluster.workers[workerID].send(event)
}

const configureWorkers = async (numWorkers: number) => {
    const workerQueue = new Queue()
    const processOrder = new Queue()

    for (let i = 0; i < numWorkers; i++) {
        const worker = cluster.fork()

        worker.on(clusterEvents.MESSAGE, (message: MapReduceEvent) => {
            const {id, table, SYN, SYN_ACK} = message;

            getLogger(getLoggerID())(`Received events from worker: ${JSON.stringify(message)}`)

            if (SYN) {
                // Return signal to worker to start processing
                worker.send({ACK: true})
            } else if (SYN_ACK || id) {
                workerQueue.enqueue(worker.id)
                getLogger(getLoggerID())(`Worker ${worker.id} now available`)
            }

            if (table) {
                processOrder.enqueue(table)
            }

        })

        worker.on(clusterEvents.DISCONNECT, () => {
            getLogger(getLoggerID())(`Gracefully shutting down worker #${worker.id}`)
        })
    }

    getLogger(getLoggerID())("Worker queues initializing")
    while (workerQueue.getElements().length != numWorkers) {
        await Delay()
    }
    getLogger(getLoggerID())(`Workers queue populated`)

    return {workerQueue, processOrder}
}

export const MapReduce = async (metadata: DBMetadataGraph, adjMatrix: Result, config: Options, numWorkers = NUM_CPUS) => {
    if (isMaster()) {
        await initMaster(adjMatrix)

        getLogger(getLoggerID())("Running Map reduce");
        getLogger(getLoggerID())(`Process running on pid ${pid}`);

        const {workerQueue, processOrder} = await configureWorkers(numWorkers)

        for (const [root, dependencies] of Object.entries(adjMatrix)) {
            for (let i = 0; i < getMaxDependencyCount(dependencies); i++) {
                for (let j = 0; j < dependencies.length; j++) {
                    if (i < dependencies[j].length) {
                        await Map(workerQueue, {table: dependencies[j][i]})
                    }
                }
            }

            await Map(workerQueue, {table: root})
        }

        await shutdown(processOrder);

    } else if (cluster.isWorker) {
        const workerName = getLoggerID()

        process.send({SYN: true})

        process.on(clusterEvents.MESSAGE, async (event: MapReduceEvent) => {
            if (event.ACK) {
                getLogger(getLoggerID())(`Worker ${cluster.worker.id} now active and processing requests`)

                process.send({SYN_ACK: true})
                return
            }

            const data = {id: cluster.worker.id, table: event.table, workerName}
            const dbInstance = getDBInstance(config)

            getLogger(workerName)(`Processing table ${event.table}`)

            try {
                await ensureDependenciesSatisfied(metadata, event.table)
                await dbInstance.sequelize.query(`DROP TABLE IF EXISTS "${event.table}" CASCADE;\n` + readBackup(event.table), {
                    type: QueryTypes.INSERT,
                    benchmark: false,
                    logging: false
                })
                await Tables.update({isProcessed: true}, {where: {name: event.table, isProcessed: false}})

                getLogger(workerName)(`Writing event ${JSON.stringify(event)} to master`)

                process.send(data)
            } catch (err) {
                getLogger(workerName)(`Error processing table ${event.table}: ${err}`)
            } finally {
                await dbInstance.sequelize.close()
            }
        })
    }
}

(async () => {
    const {metadata, results} = restore()

    const numWorkers = 4
    const label = `worker-count: #${numWorkers}`

    console.time(label)
    await MapReduce(metadata, results, config as Options, numWorkers)
    console.timeEnd(label)
})();
