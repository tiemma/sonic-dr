import {readFileSync} from "fs";

export const readBackup = (table: string) => readFileSync(`${process.cwd()}/backup/${table}.sql`).toString()

export const Delay = (time = Math.random() * 50) => new Promise(resolve => setTimeout(resolve, time))

export const getLogger = (loggerID: string) => (message: any, date = new Date().toISOString()) => console.log(`${date}: ${loggerID}: ${message}`)
