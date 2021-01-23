const express = require('express');
const hbase = require('hbase')

const hbaseManager = require('../db/hbaseManager');

const router = express.Router();
const client = hbaseManager.client;

router.get("/", (req, res) => {
    res.render("influencers");
});

module.exports = router;