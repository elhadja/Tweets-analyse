const express = require('express')
const app = express()
var eclairjs = require('eclairjs');
const ejs = require('ejs');
const path = require('path');

const hbaseManager = require('./db/hbaseManager');

hbaseManager.checkTablesAccessiblity();

const userRoutes = require('./routes/user');
const hashtagRoutes = require('./routes/hashtag');
const influencersRoutes = require('./routes/influencer');


app.set('view engine', 'ejs');
app.get('/', function (req, res) {
  res.render("home");
});
app.use('/user', userRoutes);
app.use('/hashtag', hashtagRoutes);
app.use('/influencer', influencersRoutes);

app.listen(3903, () => {
  console.log("app listening at " + 3903);
});