package bigdata;

import java.io.Serializable;

public class Tweet implements Serializable{
    public String created_at = "";
    public String id = "";
    public String id_str = "";
    public String text = "";
    private String source;
    private boolean truncated;
    public User user;
    private String geo;
    private String coordinates;
    private String place;
    private String contributors;
    private boolean is_quote_status;
    private long quote_count;
    private long reply_count;
    private long retweet_count;
    private long favourites_count;
    public Entity entities;
    private boolean favorited;
    private boolean retweeted;
    private String filter_level;
    public String lang = "";
    private String timestamp_ms;

    public static int counter = -1;
    public int row;

    public Tweet() {
        Tweet.counter += 1;
        row = counter;
    }

    public String getLang() {
        return lang;
    }


   public String toString() {
        return "created_at: " + created_at + 
                "\nid: " + id +
                "\nid_string: " + id_str +
                "\ntext: " + text +
                "\nsouce: " + source + 
                "\ntruncated: " + truncated +
                "\nuser:" +
                user.toString() +
                "\ngeo: " + geo +
                "\ncoordinates: " + coordinates +
                "\nplace: " + place +
                "\ncontributors: " + contributors +
                "\nis_quote_status: " + is_quote_status +
                "\nquote_count: " + quote_count +
                "\nreply_count: " + reply_count +
                "\nretweet_count: " + retweet_count +
                "\nfavourites_count: " + favourites_count +
                "\nentities:" +
                entities.toString() +
                "\nfavourited: " + favorited + 
                "\nretweeted: " + retweeted + 
                "\nfilter_level: " + filter_level + 
                "\nlang: " + lang + 
                "\ntimestamp_ms: " + timestamp_ms; 
    }
}