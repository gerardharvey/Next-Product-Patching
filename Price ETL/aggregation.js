const pipeline = [
  {
    $match:
      {
        "itemID": { 
          $in: [
            "xad118",
            "ack1",
            "fkt24",
            "izy29",
            "toq56",
            "aon68",
            "gvr88",
            "kga98",
            "hnh120",
            "aht126",
            "xqy134",
            "iet156",
            "uix168"            
          ]
         },      
        "globalTerritories.priceGroupCode": "MX"
      }
  },
  {
    $set:
      {
        globalTerritories: {
          $filter: {
            input: "$globalTerritories",
            as: "territory",
            cond: {
              $eq: [
                "$$territory.priceGroupCode",
                "MX"
              ]
            }
          }
        }
      }
  }
];

const results = db.Prices.aggregate(pipeline).explain("executionStats");
console.log(results);

