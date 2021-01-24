const hbase = require('hbase')

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

function checkTablesAccessiblity() {
    hbase()
    .table(TABLE_USER_HASHTAGS)
    .schema(function(error, schema){
    if (!schema) {
        console.log("error: " + TABLE_USER_HASHTAGS + " is not reached");
    }
    });

    hbase()
    .table(TABLE_NUMBER_TWEETS)
    .schema(function(error, schema){
    if (!schema) {
        console.log("error: " + TABLE_NUMBER_TWEETS + " is not reached");
    }
    });

    hbase()
    .table(TABLE_NUMBER_TWEETS_BY_LANG)
    .schema(function(error, schema){
    if (!schema) {
        console.log("error: " + TABLE_NUMBER_TWEETS_BY_LANG + " is not reached");
    }
    });

    hbase()
    .table(TABLE_USERS_BY_HASHTAG)
    .schema(function(error, schema){
    if (!schema) {
        console.log("error: " + TABLE_USERS_BY_HASHTAG + " is not reached");
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
}

exports.client = client;
exports.TABLE_NUMBER_TWEETS = TABLE_NUMBER_TWEETS;
exports.TABLE_NUMBER_TWEETS_BY_LANG = TABLE_NUMBER_TWEETS_BY_LANG;
exports.TABLE_USER_HASHTAGS = TABLE_USER_HASHTAGS;
exports.TABLE_USERS_BY_HASHTAG = TABLE_USERS_BY_HASHTAG;
exports.TABLE_TOPK_HASHTAGS = TABLE_TOPK_HASHTAGS;
exports.TABLE_TOPK_HASHTAGS_By_DAY = TABLE_TOPK_HASHTAGS_By_DAY;

exports.checkTablesAccessiblity = checkTablesAccessiblity;