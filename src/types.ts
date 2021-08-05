import {Queue} from "@tiemma/sonic-core";

export type StringArrMap = { [key: string]: Set<string> };

export type QueryData = { "data": StringArrMap };

export interface Scheduler {
    processNow: Queue;
    processLater: Queue;
}

export type Result = { [key: string]: string[][] }