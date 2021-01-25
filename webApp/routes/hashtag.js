const express = require('express');
const hbase = require('hbase')

const hbaseManager = require('../db/hbaseManager');

const router = express.Router();
const client = hbaseManager.client;

router.get("/", (req, res) => {
    res.render("hashtags");
});

router.get('/usersByHashtag/:hashtagId', (req, res) => {
  const hashtagId = req.params.hashtagId;
  hbase()
    .table(hbaseManager.TABLE_USERS_BY_HASHTAG)
    .row(hashtagId)
    .get({from: 1285942515900}, (error, value) => {
      if (!error) {
        try {
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

router.get('/count/:hashtagId', (req, res) => {
  const hashtagId = req.params.hashtagId;
  hbase()
    .table(hbaseManager.TABLE_USERS_BY_HASHTAG)
    .row(hashtagId)
    .get({from: 1285942515900}, (error, value) => {
      if (!error) {
        try {
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

router.get('/topk/:k', (req, res) => {
  const k = req.params.k;
  client
  .table(hbaseManager.TABLE_TOPK_HASHTAGS)
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

router.get('/topkByDay/:day/:k', (req, res) => {
  const k = req.params.k;
  const day = req.params.day;
  client
  .table(hbaseManager.TABLE_TOPK_HASHTAGS_By_DAY)
  .scan({
    filter: {
    "op":"MUST_PASS_ALL","type":"FilterList","filters":[{
        "op":"EQUAL",
        "type":"RowFilter",
        "comparator":{"value": day + "-.+","type":"RegexStringComparator"}
      }
    ]}
  }, (err, rows) => {
    if (!err) {
      rows = rows.sort((o1, o2) => {return o1.key.replace("-", '') - o2.key.replace("-", '')}).slice(0, parseInt(k, 10)*2);
      topkByDay = {
        hashtags: rows
      }
      console.log(rows);
      res.status(200).render("hashtags", {topkByDay});
    } else {
      res.json("erreur");
    }
  }) 
});

module.exports = router;