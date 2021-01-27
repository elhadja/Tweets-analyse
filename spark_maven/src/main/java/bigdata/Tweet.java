package bigdata;

import java.io.Serializable;

public class Tweet implements Serializable{
    private String created_at = "";
    private String id = "";
    private String lang = "";

    private User user;
    private Entity entities;

    public String getLang() {
        return lang;
    }

    public String getId() {
        return id;
    }

    public User getUser() {
        return user;
    }

    public Entity getEntity() {
        return entities;
    }

   public String toString() {
        return "created_at: " + created_at + 
                "\nid: " + id +
                "\nuser:" +
                user.toString() +
                "\nentities:" +
                entities.toString() +
                "\nlang: " + lang; 
    }
}