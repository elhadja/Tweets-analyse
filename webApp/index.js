const express = require('express')
const app = express()
var eclairjs = require('eclairjs');
const hbase = require('hbase')
const ejs = require('ejs');
const path = require('path');
var spark = new eclairjs();

const TABLE_NAME = 'elhadj_tweet';
const TABLE_NUMBER_TWEETS = 'bah_simba';

//var sc = new spark.SparkContext("local[*]", "Simple Spark Program");
//var data = sc.parallelize([1.10, 2.2, 3.3, 4.4]);

// hbase js
const client = hbase({
  host: '127.0.0.1',
  port: 8080
})

hbase()
.table(TABLE_NAME)
.schema(function(error, schema){
  //console.info(schema)
});

const myRow = new hbase.Row(client, TABLE_NAME, '11235')
console.log(myRow);
console.log(myRow.get("created_at", (error, value) => {
  console.log(value);
}));

const myScanner = new hbase.Scanner(client, {table: TABLE_NAME})
//console.log(myScanner);

// fin hbase js

app.set('view engine', 'ejs');

app.get('/', function (req, res) {
  res.render("home");
});

app.get('/user/tweets', (req, res) => {
  console.log("count: " + req.query.idUserInput);
  const userId = req.query.idUserInput;

  hbase()
    .table(TABLE_NUMBER_TWEETS)
    .row(userId)
    .get({from: 1285942515900}, (error, value) => {
      try {
        res.json({name: value[0].$, numberTweets: value[1].$});
      } catch (error) {
        res.json({error: "no such rows"})
      }
    })
});

app.get('/row/:rowKey', (req, res) => {
  hbase()
    .table(TABLE_NAME)
    .row(req.params.rowKey)
    .get('text', {from: 1285942515900}, (error, value) => {
      try {
        res.status(200).json(value[0].$);
      } catch (error) {
        res.json({error: "no such rows"})
      }
    })
})
 
app.listen(3903)