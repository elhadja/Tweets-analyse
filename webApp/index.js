const express = require('express')
const app = express()
var eclairjs = require('eclairjs');
const hbase = require('hbase')
const ejs = require('ejs');
const path = require('path');
var spark = new eclairjs();

const TABLE_NAME = 'elhadj_tweet';
const TABLE_NUMBER_TWEETS = 'bah-simba_tweets_by_user';

const client = hbase({
  host: '127.0.0.1',
  port: 8080
})

// check if hbase is accessible
hbase()
.table(TABLE_NUMBER_TWEETS)
.schema(function(error, schema){
  if (!schema) {
    console.log("error: bbase is not reached :-(");
  } else {
    console.log("bbase is reached");
  }
});

app.set('view engine', 'ejs');

app.get('/', function (req, res) {
  res.render("home");
});

app.get('/user/tweets/:userId', (req, res) => {
  const userId = req.params.userId;
  console.log("coucou: " + userId)
  hbase()
    .table(TABLE_NUMBER_TWEETS)
    .row(userId)
    .get({from: 1285942515900}, (error, value) => {
      console.log("hehe")
      if (!error) {
        try {
          userInfos = {
            name: value[0].$,
            numberTweets: value[1].$
          };
          console.log("before")
          res.render("home", {userInfos})
          console.log("after")
        } catch (error) {
          res.json({error: "no such rows"})
        }
      }
      else {
        console.log("error: ", error);
        res.json(error);
      }
    })
});
 
app.listen(3903)