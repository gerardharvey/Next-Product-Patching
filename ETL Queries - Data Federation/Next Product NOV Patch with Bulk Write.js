/* 
AUTHOR: Gerard Harvey
COMPANY: MongoDB
DESCRIPTION: This script demonstrates how to use MongoDB's bulkWrite operation to update a large number of documents in a target 
  collection based on data from a Atlas Data Federation Virtual Collection that is reading NOV data from an S3 bucket (configured 
  within Data Federation itself). 

DEPENDENCIES:
  - process.env.MONGO_CLUSTER_CONNECTION_STRING must be set to a connection string for the target cluster that contains the "Next" 
    database and "Products" collection to be updated
  - process.env.MONGO_DATAFED_CONNECTION_STRING must be set to a connection string for the Data Federation instance that can read 
    from the S3
  - npm install mongodb should be run to install the MongoDB NodeJS driver
  - The Data Federation instance must be configured with a service that can read from the S3 bucket containing the NOV data, and the 
    patch collection must be populated with the relevant data for the script to process

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

const getProductCollection = function(dbName) {
  if(clusterClient==null) {
    clusterClient = new MongoClient(process.env.MONGO_CLUSTER_CONNECTION_STRING);
    clusterClient.connect();
  }
  const products = clusterClient.db(dbName).collection("Products");
  return products;
}

const bulkWrite = async function(collection, updates) {
  const batchSize = 100;
  let total = 0;
  for (let i = 0; i < updates.length; i += batchSize) {
    const batch = updates.slice(i, i + batchSize);
    const result = await collection.bulkWrite(batch);
    total += result.modifiedCount;
    console.log(`Processed batch of ${batch.length} updates out of ${updates.length}, total modified: ${total}`);
  }
  return total;
}

const trigger = async function() {
  const patchSvcName = process.env.MONGO_DATAFED_CONNECTION_STRING;
  const patchDBName = "NextDemoPatches";
  const patchCollName = "novPatches";
  console.log(`Starting NextProductPatch... connecting to ${patchSvcName}...`);
  const patchColl = getCollection(patchSvcName, patchDBName, patchCollName);

  try {
    const pipeline = [
      { $match: { next_order_value: { $ne: 0}}},
      { $set: { sku: '$item_number'}},
      { $project: {
          sku: 1,
          next_order_value: 1,
          TiSeg1: 1,
          TiSeg2: 1,
          TiSeg3: 1,
          TiSeg4: 1,
          TiSeg5: 1,
          file_date: 1        
      }},
      { $limit: 1 }
    ];

    const productColl = getProductCollection("Next");
    const novCursor = await patchColl.aggregate(pipeline).toArray();
    var updates = [];
    for(let i = 0; i < novCursor.length; i++) {
      const doc = novCursor[i];
      updates.push({
        updateOne: {
          filter: { 'sku': `${doc.sku}` },
          update: {
            $set: {
              next_order: doc.next_order_value,
              TiSeg1: `${doc.TiSeg1}`,
              TiSeg2: `${doc.TiSeg2}`,
              TiSeg3: `${doc.TiSeg3}`,
              TiSeg4: `${doc.TiSeg4}`,
              TiSeg5: `${doc.TiSeg5}`,
              file_date: `${doc.file_date}`  
            }
          }
        }
      });
    };
    return await bulkWrite(productColl, updates);
    
  } catch (err) {
    console.log("error executing trigger: ", err.message);
  }
};

trigger();