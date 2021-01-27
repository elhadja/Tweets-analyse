package bigdata;

import java.io.Serializable;

public class User implements Serializable {
    private String id = "";
    private String id_str;
    private String name = "";

    private int numberTweets = 1;
    private String hashtags;

    public String getName() {
        return this.name;
    }

    public String getId() {
        return this.id;
    }

    public int getNumberTweets() {
        return numberTweets;
    }

    public String getHashtags() {
        return hashtags;
    }

    public void addHashtag(String hashtag) {
        this.hashtags = hashtag;
    }

    public void mergeHashtags(User other) {
        this.hashtags += (other.hashtags + ",");
    }

    public void mergeNumberTweets(User other) {
        this.numberTweets += other.getNumberTweets();
    }

    public String toString() {
        return 
                "\n\tid: " + id +
                "\n\tid_str: " + id_str +
                "\n\tname: " + name;
    }

}