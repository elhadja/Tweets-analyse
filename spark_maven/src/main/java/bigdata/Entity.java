package bigdata;

import java.io.Serializable;

import java.util.List;
import java.util.ArrayList;

public class Entity implements Serializable{
    public List<HashTag> hashtags = new ArrayList<>();

    public List<HashTag> getHashtags() {
        return hashtags;
    }

    public String hastagsToString() {
        String hastagsString = "";
        for (HashTag ht : hashtags) {
            hastagsString += ht.getText() + ",";
        }
        return hastagsString;
    }

    public String toString() {
        return  "\n\thastags: " + hashtags;
    }
}
