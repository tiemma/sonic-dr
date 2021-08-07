import {backup, restore} from "./strategy";


(async () =>{
    await backup()
    await restore()
} )();
