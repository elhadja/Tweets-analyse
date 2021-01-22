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
const TABLE_USERS_BY_HASHTAG = 'bah-simba_users_by_hashtag';
const TABLE_TOPK_HASHTAGS = 'bah-simba_topK_hashtags';
const TABLE_TOPK_HASHTAGS_By_DAY = 'bah-simba_topK_hashtags_by_day';

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

hbase()
.table(TABLE_USERS_BY_HASHTAG)
.schema(function(error, schema){
  if (!schema) {
    console.log("error: " + TABLE_USERS_BY_HASHTAG + " is not reached");
  } else {
    console.log(TABLE_USERS_BY_HASHTAG + " is reached");
  }
});

hbase()
.table(TABLE_TOPK_HASHTAGS)
.schema(function(error, schema){
  if (!schema)
    console.log("error: " + TABLE_TOPK_HASHTAGS + " is not reached");
});

hbase()
.table(TABLE_TOPK_HASHTAGS_By_DAY)
.schema(function(error, schema){
  if (!schema)
    console.log("error: " + TABLE_TOPK_HASHTAGS_By_DAY + " is not reached");
});



app.set('view engine', 'ejs');

app.get('/', function (req, res) {
  res.render("home");
});

app.get('/hashtags', function (req, res) {
  res.render("hashtags");
});


app.get('/influencers', function (req, res) {
  res.render("influencers");
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

app.get('/hashtag/usersByHashtag/:hashtagId', (req, res) => {
  const hashtagId = req.params.hashtagId;
  console.log(hashtagId);
  hbase()
    .table(TABLE_USERS_BY_HASHTAG)
    .row(hashtagId)
    .get({from: 1285942515900}, (error, value) => {
      if (!error) {
        try {
         console.log(value); 
         let users = {
           value: value[1].$.split(",")
         }
         res.render("hashtags", {users})
        } catch (error) {
          res.json({error: "no such rows"})
        }
      }
      else {
        console.log("error when getting row: ", error);
        res.json(error);
      }
    })
 
});

app.get('/hashtag/count/:hashtagId', (req, res) => {
  const hashtagId = req.params.hashtagId;
  console.log(hashtagId);
  hbase()
    .table(TABLE_USERS_BY_HASHTAG)
    .row(hashtagId)
    .get({from: 1285942515900}, (error, value) => {
      if (!error) {
        try {
         console.log(value); 
         let countResponse = {
           value: value[0].$
         }
         res.render("hashtags", {countResponse})
        } catch (error) {
          res.json({error: "no such rows"})
        }
      }
      else {
        console.log("error when getting row: ", error);
        res.json(error);
      }
    })
 
});

app.get('/hashtag/topk/:k', (req, res) => {
  const k = req.params.k;
  client
  .table(TABLE_TOPK_HASHTAGS)
  .scan({}, (err, rows) => {
    if (!err) {
      rows = rows.sort((o1, o2) => {return o1.key - o2.key}).slice(0, parseInt(k, 10)*2);
      topk = {
        hashtags: rows
      }
      res.status(200).render("hashtags", {topk});
    } else {
      res.json("erreur");
    }
  }) 

});

app.get('/hashtag/topkByDay/:day/:k', (req, res) => {
  const k = req.params.k;
  const day = req.params.day;
  client
  .table(TABLE_TOPK_HASHTAGS_By_DAY)
  .scan({}, (err, rows) => {
    if (!err) {
      rows = rows.sort((o1, o2) => {return o1.key - o2.key}).slice(0, parseInt(k, 10)*2);
      topkByDay = {
        hashtags: rows
      }
      res.status(200).render("hashtags", {topkByDay});
    } else {
      res.json("erreur");
    }
  }) 

});

 
app.listen(3903)