const express = require('express')
const app = express()
var eclairjs = require('eclairjs');
const hbase = require('hbase')
const ejs = require('ejs');
const path = require('path');
var spark = new eclairjs();

const TABLE_NAME = 'elhadj_tweet';
const TABLE_NUMBER_TWEETS = 'bah-simba_tweets_by_user';
const TABLE_NUMBER_TWEETS_BY_LANG = 'bah-simba_tweets_by_lang';
const TABLE_USER_HASHTAGS = 'bah-simba_users_hashtags';

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

hbase()
.table(TABLE_NUMBER_TWEETS_BY_LANG)
.schema(function(error, schema){
  if (!schema) {
    console.log("error: " + TABLE_NUMBER_TWEETS_BY_LANG + " is not reached");
  } else {
    console.log("bbase table_tweets_by_lang is reached");
  }
});


app.set('view engine', 'ejs');

app.get('/', function (req, res) {
  res.render("home");
});

app.get("/tweetsByLang", (req, res) => {
  //*
  client
  .table(TABLE_NUMBER_TWEETS_BY_LANG)
  .scan({}, (err, rows) => {
    if (!err) {
      console.info(rows)
      tweetsByLang = {
        list: rows
      };
      res.render("home", {tweetsByLang});
    } else {
      res.json("erreur");
    }
  }) 
  //*/
});

function getRow(tableName, rowKey) {
  console.log("in func");
  hbase()
      .table(tableName)
      .row(rowKey)
      .get({from: 1285942515900}, (error, value) => {
        if (!error) {
          try {
            console.log(value);
          } catch (error) {
            console.log("no such rows");
          }
        }
        else {
          console.log("error: ", error);
        }
      })
  console.log("after func");

}

app.get('/user/tweets/:userId', (req, res) => {
  const userId = req.params.userId;
  hbase()
    .table(TABLE_NUMBER_TWEETS)
    .row(userId)
    .get({from: 1285942515900}, (error, value) => {
      if (!error) {
        try {
          userInfos = {
            name: value[0].$,
            numberTweets: value[1].$
          };
          res.render("home", {userInfos})
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

app.get('/user/userHashtags/:userId', (req, res) => {
  const userId = req.params.userId;
  hbase()
    .table(TABLE_USER_HASHTAGS)
    .row(userId)
    .get({from: 1285942515900}, (error, value) => {
      if (!error) {
        try {
         console.log(value); 
         let userHashtags = {
           hashtags: value[0].$
         }
         res.render("home", {userHashtags})
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