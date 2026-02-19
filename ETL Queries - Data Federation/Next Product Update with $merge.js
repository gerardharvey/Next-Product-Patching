/* 
AUTHOR: Gerard Harvey
COMPANY: MongoDB
DESCRIPTION: This script demonstrates how to use MongoDB's $merge operation to update a large number of Products using data held 
  within a Atlas Data Federation Virtual Collection. A $merge is used to preserve the Atlas Search Index created on the target
  collection, which would be dropped and recreated if $out was used instead. 

DEPENDENCIES:
  - process.env.MONGO_DATAFED_PROD_CONNECTIONSTRING must be set to a connection string for the Data Federation instance that can read 
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
  const CountryCode = "DK"; // "NO", "SE"
  const patchSvcName = process.env.MONGO_DATAFED_PROD_CONNECTIONSTRING;
  const patchDBName = "NextProductDatabase";
  const patchCollName = `Products${CountryCode}`;
  const variantFieldName = `variants${CountryCode}`;
  console.log(`Starting ${CountryCode} Product Load at ${startTime}... connecting to ${patchSvcName}...`);
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
            "_id": 0,
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
          variantsDK: "$variants",
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
      },
      { $unset: ["_id", "variants"] },
      {
        $merge: {
            into: {
              atlas: {
                projectId: "6759c201136a02253fea72a4",
                clusterName: "AzureDemo",
                db: "Next",
                coll: "ProductsNEUR"
              }
            },
            on: "sku",
            whenMatched: [ {
              $set: {
                "brand": "$$new.brand",
                "item_type": "$$new.item_type",
                "gender": "$$new.gender",
                "next_gender": "$$new.next_gender",
                "department": "$$new.department",
                "next_category": "$$new.next_category",
                "title": "$$new.title",
                "description": "$$new.description",
                "style_id": "$$new.style_id",
                "use": "$$new.use",
                "material": "$$new.material",
                "pattern": "$$new.pattern",
                "sale_keywords": "$$new.sale_keywords",
                "start_date": "$$new.start_date",
                "end_date": "$$new.end_date",
                "accessory": "$$new.accessory",
                "assembly": "$$new.assembly",
                "design": "$$new.design",
                "designfeature": "$$new.designfeature",
                "url": "$$new.url",
                "firmness": "$$new.firmness",
                "fit": "$$new.fit",
                "gift": "$$new.gift",
                "neckline": "$$new.neckline",
                "occasion": "$$new.occasion",
                "productaffiliation": "$$new.productaffiliation",
                "room": "$$new.room",
                "sleeve": "$$new.sleeve",
                "sizetype": "$$new.sizetype",
                "next_order": "$$new.next_order",
                "personalised": "$$new.personalised",
                "producttype": "$$new.producttype",
                "blindtype": "$$new.blindtype",
                "trousersize": "$$new.trousersize",
                "contentReady": "$$new.contentReady",
                "channel_code": "$$new.channel_code",
                "newin": "$$new.newin",
                "subdepartment": "$$new.subdepartment",
                "offer": "$$new.offer",
                "image": "$$new.image",
                "msp": "$$new.msp",
                "mxsp": "$$new.mxsp",
                "designerboutique": "$$new.designerboutique",
                "next_forward_forecast": "$$new.next_forward_forecast",
                "global_test_slot_1": "$$new.global_test_slot_1",
                "global_test_slot_2": "$$new.global_test_slot_2",
                "global_test_slot_3": "$$new.global_test_slot_3",
                "next_order_value_seg_1": "$$new.next_order_value_seg_1",
                "next_forward_forecast_seg_1": "$$new.next_forward_forecast_seg_1",
                "test_slot_1": "$$new.test_slot_1",
                "next_order_value_seg_2": "$$new.next_order_value_seg_2",
                "next_forward_forecast_seg_2": "$$new.next_forward_forecast_seg_2",
                "test_slot_2": "$$new.test_slot_2",
                "next_order_value_seg_3": "$$new.next_order_value_seg_3",
                "next_forward_forecast_seg_3": "$$new.next_forward_forecast_seg_3",
                "test_slot_3": "$$new.test_slot_3",
                "next_order_value_seg_4": "$$new.next_order_value_seg_4",
                "next_forward_forecast_seg_4": "$$new.next_forward_forecast_seg_4",
                "test_slot_4": "$$new.test_slot_4",
                "next_order_value_seg_5": "$$new.next_order_value_seg_5",
                "next_forward_forecast_seg_5": "$$new.next_forward_forecast_seg_5",
                "test_slot_5": "$$new.test_slot_5",
                "next_order_value_seg_6": "$$new.next_order_value_seg_6",
                "next_forward_forecast_seg_6": "$$new.next_forward_forecast_seg_6",
                "test_slot_6": "$$new.test_slot_6",
                "next_order_value_seg_7": "$$new.next_order_value_seg_7",
                "next_forward_forecast_seg_7": "$$new.next_forward_forecast_seg_7",
                "test_slot_7": "$$new.test_slot_7",
                "maternity": "$$new.maternity",
                "petitecurvetall": "$$new.petitecurvetall",
                "subbrand": "$$new.subbrand",
                "feat": "$$new.feat",
                "category_paths": "$$new.category_paths",
                "wedding": "$$new.wedding",
                "markdowntype": "$$new.markdowntype",
                "primary_colour": "$$new.primary_colour",
                "secondary_colour": "$$new.secondary_colour",
                "further_reduced": "$$new.further_reduced",
                "variantsDK": "$$new.variantsDK",
                "materialized_paths": "$$new.materialized_paths"
              }
            }],
            whenNotMatched: "insert"
        }
      }
    ];

    const rslt = await patchColl.aggregate(pipeline).toArray();    
    const endTime = new Date();
    console.log(`${CountryCode} Product Merge completed at ${endTime} with ${rslt.matchedCount} matched and ${rslt.modifiedCount} modified documents.`); 
    console.log(`Total execution time: ${(endTime.getTime() - startTime.getTime())/1000} seconds.`);
    
  } catch (err) {
    console.log("error executing trigger: ", err.message);
  }
};

trigger();