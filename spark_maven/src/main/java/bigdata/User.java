package bigdata;

import java.io.Serializable;

public class User implements Serializable{
    public String id = "";
    private String id_str;
    public String name = "";
    private String screen_name;
    private String location;
    private String url;


    private long followers_count;
    private long friends_count;
    private long listed_count;
    private long favourites_count;
    private long statuses_count;
    private String created_at;
    private String utc_offset;
    private String time_zone;

    public String getName() {
        return this.name;
    }

    public String getId() {
        return this.id;
    }

    public String toString() {
        return 
                "\n\tid: " + id +
                "\n\tid_str: " + id_str +
                "\n\tname: " + name +
                "\n\tscreen_name: " + screen_name +
                "\n\tlocation: " + location +
                "\n\turl: " + url +
                "\n\tfollowers_count: " + followers_count +
                "\n\tfriends_count: " + friends_count +
                "\n\tlisted_count: " + listed_count +
                "\n\tfavourites_count: " + favourites_count +
                "\n\tstatuses_count: " + statuses_count +
                "\n\tcreated_at: " + created_at +
                "\n\tutc_offset: " + utc_offset +
                "\n\ttime_zone: " + time_zone;
    }

}