/* 
AUTHOR: Gerard Harvey
COMPANY: MongoDB
DESCRIPTION: This script demonstrates how to use MongoDB's bulk write operation to update a large number of Products using data held 
  within a Atlas Data Federation Virtual Collection. Similar to $merge, a bulk write is used to preserve the Atlas Search Index created 
  on the target collection, which would be dropped and recreated if $out was used instead. 

DEPENDENCIES:
  - process.env.MONGO_DATAFED_PROD_CONNECTIONSTRING must be set to a connection string for the Data Federation instance that can read 
    from the S3
  - process.env.MONGO_CLUSTER_CONNECTION_STRING must be set to a connection string for the target cluster that contains the "Next" DB
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

const getProductCollection = function(dbName) {
  if(clusterClient==null) {
    clusterClient = new MongoClient(process.env.MONGO_CLUSTER_CONNECTION_STRING);
    clusterClient.connect();
  }
  const products = clusterClient.db(dbName).collection("ProductsUK");
  return products;
}

const bulkWrite = async function(collection, updates) {
  const batchSize = 1000;
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
  const startTime = new Date();
  const patchSvcName = process.env.MONGO_DATAFED_PROD_CONNECTIONSTRING
  const patchDBName = "NextProductDatabase";
  const patchCollName = "ProductsUK";
  console.log(`Starting UK Product Load at ${startTime}... connecting to ${patchSvcName}...`);
  const patchColl = getCollection(patchSvcName, patchDBName, patchCollName);

  try {
    const pipeline = [
      {
        $match: {
          op: "add"
        }
      },
      {
        $set: {
          "value.attributes.sku": {
            $substr: ["$path", 10, -1]
          },
          "value.attributes.variants": {
            $map: {
              input: {
                $objectToArray: "$value.variants"
              },
              as: "variants",
              in: {
                id: "$$variants.k",
                attributes: "$$variants.v.attributes"
              }
            }
          }
        }
      },
      {
        $replaceRoot: {
          newRoot: "$value.attributes"
        }
      },
      {
        $project: {
            "brand": 1,
            "item_type": 1,
            "gender": 1,
            "next_gender": 1,
            "department": 1,
            "next_category": 1,
            "title": 1,
            "description":1,
            "style_id": 1,
            "use": 1,
            "material": 1,
            "pattern": 1,
            "sale_keywords": 1,
            "start_date": 1,
            "end_date": 1,
            "accessory": 1,
            "assembly": 1,
            "design": 1,
            "designfeature": 1,
            "url": 1,
            "firmness": 1,
            "fit": 1,
            "gift": 1,
            "neckline": 1,
            "occasion": 1,
            "productaffiliation": 1,
            "room": 1,
            "sleeve": 1,
            "sizetype": 1,
            "next_order": 1,
            "personalised": 1,
            "producttype": 1,
            "blindtype": 1,
            "trousersize": 1,
            "contentReady": 1,
            "channel_code": 1,
            "newin": 1,
            "subdepartment": 1,
            "offer": 1,
            "image": 1,
            "msp": 1,
            "mxsp": 1,
            "designerboutique": 1,
            "next_forward_forecast": 1,
            "global_test_slot_1": 1,
            "global_test_slot_2": 1,
            "global_test_slot_3": 1,
            "next_order_value_seg_1": 1,
            "next_forward_forecast_seg_1": 1,
            "test_slot_1": 1,
            "next_order_value_seg_2": 1,
            "next_forward_forecast_seg_2": 1,
            "test_slot_2": 1,
            "next_order_value_seg_3": 1,
            "next_forward_forecast_seg_3": 1,
            "test_slot_3": 1,
            "next_order_value_seg_4": 1,
            "next_forward_forecast_seg_4": 1,
            "test_slot_4": 1,
            "next_order_value_seg_5": 1,
            "next_forward_forecast_seg_5": 1,
            "test_slot_5": 1,
            "next_order_value_seg_6": 1,
            "next_forward_forecast_seg_6": 1,
            "test_slot_6": 1,
            "next_order_value_seg_7": 1,
            "next_forward_forecast_seg_7": 1,
            "test_slot_7": 1,
            "maternity": 1,
            "petitecurvetall": 1,
            "subbrand": 1,
            "feat": 1,
            "category_paths": 1,
            "wedding": 1,
            "markdowntype": 1,
            "primary_colour": 1,
            "secondary_colour": 1,
            "further_reduced": 1,
            "sku": 1,
            "variants": 1
        }
      },
      {
        $set: {
          materialized_paths: {
            $setUnion: [
              {
                $reduce: {
                  input: "$category_paths",
                  initialValue: [],
                  in: {
                    $concatArrays: [
                      "$$value",
                      "$$this.id"
                    ]
                  }
                }
              }
            ]
          }
        }
      }      
    ];

    const productColl = getProductCollection("Next");
    const novCursor = await patchColl.aggregate(pipeline).toArray();
    var updates = [];
    console.log(`Creating bulkWrites for ${novCursor.length} documents...`);
    for(let i = 0; i < novCursor.length; i++) {
      const doc = novCursor[i];
      updates.push({
        replaceOne: {
          filter: { 'sku': `${doc.sku}` },
          replacement: doc,
          upsert: true
        }
      });
    };
    const rslt = await bulkWrite(productColl, updates);
    const endTime = new Date();
    console.log(`UK Product Load completed at ${endTime} with ${rslt.matchedCount} matched and ${rslt.modifiedCount} modified documents.`); 
    console.log(`Total execution time: ${(endTime.getTime() - startTime.getTime())/1000} seconds.`);
    
  } catch (err) {
    console.log("error executing trigger: ", err.message);
  }
};

trigger();