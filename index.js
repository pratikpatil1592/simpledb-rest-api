var
  async       = require('async'),
  aws         = require('aws-sdk'),
  bodyParser  = require('body-parser'),
  cuid        = require('cuid'),
  express     = require('express'),
  
  sdbDomain   = 'sdb-rest-tut',
  
  app         = express(),
  
  schema      = ['pets','cars','furniture','phones'],
  simpledb;

aws.config.loadFromPath(process.env['HOME'] + '/aws.credentials.json');

simpledb = new aws.SimpleDB({
  region 		: 'US-East',
  endpoint 	: 'https://sdb.amazonaws.com'
});

function attributeObjectToAttributeValuePairs(attrObj, replace) {
   var
     sdbAttributes   = [];

    Object.keys(attrObj).forEach(function(anAttributeName) {
      attrObj[anAttributeName].forEach(function(aValue) {
        sdbAttributes.push({
          Name    : anAttributeName,
          Value   : aValue,
          Replace : replace
        });
      });
    });

   return sdbAttributes; 
}

function attributeValuePairsToAttributeObject(pairs) {
  var
    attributes = {};
  
  
  pairs
    .filter(function(aPair) {
      return aPair.Name !== 'created';
    })
    .forEach(function(aPair) {
    if (!attributes[aPair.Name]) { 
      attributes[aPair.Name] = [];
    }
    
    attributes[aPair.Name].push(aPair.Value);
  });
  
  return attributes;
}

//Create
app.post(
  '/inventory', 
  bodyParser.json(),
  function(req,res,next) {
    var
      newItemName     = cuid(),
      newAttributes   = attributeObjectToAttributeValuePairs(req.body, false);
    
    newAttributes = newAttributes.filter(function(anAttribute) {
      return schema.indexOf(anAttribute.Name) !== -1;
    });
    
    newAttributes.push({
      Name    : 'created',
      Value   : '1'
    });
      
    simpledb.putAttributes({
      DomainName    : sdbDomain,
      ItemName      : newItemName,
      Attributes    : newAttributes
    }, function(err,awsResp) {
      if (err) { 
        next(err);  //server error to user
      } else {
        res
          .status(201)
          .send({
            itemName  : newItemName
          });
      }
    });
  }
);

//Read
app.get('/inventory/:itemID', function(req,res,next) {
  simpledb.getAttributes({
    DomainName    : sdbDomain,
    ItemName      : req.params.itemID
  }, function(err,awsResp) {
    if (err) { 
      next(err);
    } else {

      if (!awsResp.Attributes) {
        res.status(404).end();
      } else {
        res.send({
          itemName    : req.params.itemID,
          inventory   : attributeValuePairsToAttributeObject(awsResp.Attributes)
        });
      }
      
    }
  });
});

//Update
app.put(
  '/inventory/:itemID', 
  bodyParser.json(),
  function(req,res,next) {
    var
      updateValues  = {},
      deleteValues  = [];
    
    schema.forEach(function(anAttribute) {
      if ((!req.body[anAttribute]) || (req.body[anAttribute].length === 0)) {
        deleteValues.push({ Name : anAttribute});
      } else {
        updateValues[anAttribute] = req.body[anAttribute];
      }
    });
    
    async.parallel([
        function(cb) {
          simpledb.putAttributes({
              DomainName    : sdbDomain,
              ItemName      : req.params.itemID,
              Attributes    : attributeObjectToAttributeValuePairs(updateValues,true),
              Expected      : {
                Name          : 'created',
                Value         : '1',
                Exists        : true
              }
            },
            cb
          );
        },
        function(cb) {
          simpledb.deleteAttributes({
              DomainName    : sdbDomain,
              ItemName      : req.params.itemID,
              Attributes    : deleteValues
            },
            cb
          );
        }
      ],
      function(err) {
        if (err) {
          next(err);
        } else {
          res.status(200).end();
        }
      }
    );
  }
);

//Delete
app.delete(
  '/inventory/:itemID', 
  function(req,res,next) {
    var
      attributesToDelete;
    
    attributesToDelete = schema.map(function(anAttribute){
      return { Name : anAttribute };
    });
    
    attributesToDelete.push({ Name : 'created' });
    
    simpledb.deleteAttributes({
        DomainName    : sdbDomain,
        ItemName      : req.params.itemID,
        Attributes    : attributesToDelete
      },
      function(err) {
        if (err) {
          next(err);
        } else {
          res.status(200).end();
        }
      }
    );
  }
);

//List
app.get(
  '/inventory',
  function(req,res,next) {
    simpledb.select({
      SelectExpression  : 'select * from `sdb-rest-tut` limit 100'
    },
    function(err,awsResp) {
      var
        items = [];
      if (err) {
        next(err);
      } else {
        items = awsResp.Items.map(function(anAwsItem) {
          var
            anItem;
          
          anItem = attributeValuePairsToAttributeObject(anAwsItem.Attributes);
          
          anItem.id = anAwsItem.Name;
          
          return anItem;
        });
        res.send(items);
      }
    });
  }
);

app.listen(3000, function () {
  console.log('SimpleDB-powered REST server started.');
});