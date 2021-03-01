"use strict";

const fs = require("fs");
const csv = require("csv-parser");
const createCsvWriter = require("csv-writer").createObjectCsvWriter;

const results = [];
const typeFilter = 'PAS';
const type0Filter = 'PAS_BRD_DEP';
const countryFilter = 'EU';
// const keys = Array.from({ length: (2020 - 2004) + 1}, (_, i) => 2004 + i)
const keys = [ 2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012, 2011, 2010, 2009, 2008, 2007, 2006, 2005, 2004];

//  INITIALIZE WRITER ----------------------------------------------------------
const csvWriter = createCsvWriter({
  path: "../out/cleaned_planes.csv",
  header: [
    //{ id: "type", title: "type" },
    //{ id: "type0", title: "type0" },
    { id: "origin", title: "origin" },
    { id: "destination", title: "destination" },
    ...keys.map((k) => ({ id: k, title: k })),
  ],
});

//  PREPARE DATA ---------------------------------------------------------------
const prepareData = (data) => {
  const { "unit,tra_meas,partner,geo_time": d1, ...rest } = data;

  // Split first column;
  const [unit, tra_meas, partner, geo_time] = d1.split(",");

  if(unit === typeFilter && tra_meas == type0Filter && !partner.includes(countryFilter) && !geo_time.includes(countryFilter)) {
    // Keep only years;
    for (const [key, value] of Object.entries(rest)) {
      if (keys.includes(Number(key))) {
        rest[Number(key)] = value;
      }
      delete rest[key];
    }

    // Convert empty to null
    for (const [key, value] of Object.entries(rest)) {
      rest[key] = value.includes(":") ? 'null' : parseInt(value);
    }

    const formated = {
      //type: unit,
      //type0: tra_meas,
      origin: partner,
      destination: geo_time,
      ...rest,
    };

    results.push(formated);
    
  }
};

//  WRITE TO CSV FILE ----------------------------------------------------------
const writeCSV = (results) => {
  csvWriter
      .writeRecords(results)
      .then(() => {
        console.log(' ')
        console.log("The CSV file was written successfully")
        console.log(' ')
        console.log(`---------- COLUMNS ----------`);
        console.log(' ')
        console.log(`${Object.keys(results[0]).length} columns saved`);
        console.log(' ')
        console.log(`${JSON.stringify(Object.keys(results[0]))}`);
        console.log(' ')
        console.log(`---------- ROWS ----------`);
        console.log(' ')
        console.log(`${results.length} rows saved`);
        console.log(' ')
        console.log(JSON.stringify(results[0], null, 2));
        console.log(' ')
        console.log(`--------------------------`);
      
      });
};


//  READ STREAM ----------------------------------------------------------------
fs.createReadStream("../data/plane_trips.tsv")
  .pipe(csv({ separator: "\t" }))
  .on("data", (data) => {
    prepareData(data);
  })
  .on("end", () => {
    writeCSV(results);
  });
