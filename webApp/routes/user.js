const express = require('express');
const hbase = require('hbase')

const hbaseManager = require('../db/hbaseManager');

const router = express.Router();
const client = hbaseManager.client;

router.get('/tweets/:userId', (req, res) => {
  const userId = req.params.userId;
  hbase()
    .table(hbaseManager.TABLE_NUMBER_TWEETS)
    .row(userId)
    .get({from: 1285942515900}, (error, value) => {
      if (!error) {
        try {
          const userInfos = {
            name: value[0].$,
            numberTweets: value[1].$
          };
          res.render("home", {userInfos})
        } catch (error) {
          res.json({error: "no such rows"})
        }
      }
      else {
        error = {
          value: "user not found"
        }
        res.render("home", {error});
      }
    })
});

router.get('/userHashtags/:userId', (req, res) => {
  const userId = req.params.userId;
  hbase()
    .table(hbaseManager.TABLE_USER_HASHTAGS)
    .row(userId)
    .get({from: 1285942515900}, (error, value) => {
      if (!error) {
        try {
         let userHashtags = {
           hashtags: value[0].$
         }
         res.render("home", {userHashtags})
        } catch (error) {
          res.json({error: "no such rows"})
        }
      }
      else {
        const errorList = {
          value: "user not found"
        }
        res.render("home", {errorList});
      }
    })

});

router.get("/tweetsByLang", (req, res) => {
  client
  .table(hbaseManager.TABLE_NUMBER_TWEETS_BY_LANG)
  .scan({}, (err, rows) => {
    if (!err) {
      tweetsByLang = {
        list: rows.sort((a, b) => b.$ - a.$)
      };
      res.render("home", {tweetsByLang});
    } else {
      res.json("erreur");
    }
  }) 
});

module.exports = router;