import {Database} from "./models";

(async() => console.log((await Database.getPostgresDBMetadata())))()