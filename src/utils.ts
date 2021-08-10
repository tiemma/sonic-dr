export const Delay = (time = Math.random() * 50) =>
  new Promise((resolve) => setTimeout(resolve, time));

export const getLogger =
  (loggerID: string) =>
  (message: any, date = new Date().toISOString()) => {
    if (process.env["QUIET"]) return;
    console.log(`${date}: ${loggerID}: ${message}`);
  };
