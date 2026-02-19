/* 
AUTHOR: Gerard Harvey
COMPANY: MongoDB
DESCRIPTION: This script demonstrates how to use MongoDB's $merge operation to update a large number of Products for NOV Scores. 

DEPENDENCIES:
  - process.env.MONGO_DATAFED_CONNECTION_STRING must be set to a connection string for the Data Federation instance that can read 
    from the S3
  - npm install mongodb should be run to install the MongoDB NodeJS driver
  - The Data Federation instance must be configured with a service that can read from the S3 bucket containing the Product data
  - Within Atlas Data Federation the target cluster and database must be set up as a Data Source

NOTE: NOT TO BE USED IN PRODUCTION. THIS IS A DEMONSTRATION ONLY
*/
import { MongoClient } from 'mongodb';

var mongoClient = null;
var clusterClient = null;

const getCollection = function(serviceName, dbName, collName) {
  if(mongoClient==null) {
    mongoClient = new MongoClient(serviceName);
    mongoClient.connect();
  }
  return mongoClient.db(dbName).collection(collName);
}

const trigger = async function() {
  const startTime = new Date();
  const patchSvcName = process.env.MONGO_DATAFED_CONNECTION_STRING;
  const patchDBName = "NextDemoPatches";
  const patchCollName = "novPatches";
  console.log(`Starting NOV patch at ${startTime}... connecting to ${patchSvcName}...`);
  const patchColl = getCollection(patchSvcName, patchDBName, patchCollName);

  try {
    const pipeline = [
      {
        '$match': {
          'next_order_value': {
            '$ne': 0
          }
        }
      }, 
      {
        $set: {
          'sku': '$item_number'
        }
      },
      {
        '$project': {
          '_id': 0, 
          'sku': 1, 
          'next_order': 1, 
          'TiSeg1': 1, 
          'TiSeg2': 1, 
          'TiSeg3': 1, 
          'TiSeg4': 1, 
          'TiSeg5': 1, 
          'TiSeg6': 1, 
          'TiSeg7': 1, 
          'TiSeg8': 1, 
          'TiSeg9': 1, 
          'TiSeg10': 1, 
          'TiSeg11': 1, 
          'TiSeg12': 1, 
          'file_date': 1
        }
      }, {
        '$merge': {
          'into': {
              'atlas': {
                'projectId': "6759c201136a02253fea72a4",
                'clusterName': "AzureDemo",
                'db': "Next",
                'coll': "ProductsNEUR"
              }
            },
          'on': 'sku', 
          'whenMatched': [
            {
              '$set': {
                'next_order': '$$new.next_order_value', 
                'TiSeg1': '$$new.TiSeg1', 
                'TiSeg2': '$$new.TiSeg2', 
                'TiSeg3': '$$new.TiSeg3', 
                'TiSeg4': '$$new.TiSeg4', 
                'TiSeg5': '$$new.TiSeg5', 
                'TiSeg6': '$$new.TiSeg6', 
                'TiSeg7': '$$new.TiSeg7', 
                'TiSeg8': '$$new.TiSeg8', 
                'TiSeg9': '$$new.TiSeg9', 
                'TiSeg10': '$$new.TiSeg10', 
                'TiSeg11': '$$new.TiSeg11', 
                'TiSeg12': '$$new.TiSeg12', 
                'file_date': '$$new.file_date'
              }
            }
          ], 
          'whenNotMatched': 'discard'
        }
      }
    ];

    const rslt = await patchColl.aggregate(pipeline).toArray();    
    const endTime = new Date();
    console.log(`NOV Patch completed at ${endTime} with ${rslt.matchedCount} matched and ${rslt.modifiedCount} modified documents.`); 
    console.log(`Total execution time: ${(endTime.getTime() - startTime.getTime())/1000} seconds.`);
    
  } catch (err) {
    console.log("error executing trigger: ", err.message);
  }
};

trigger();