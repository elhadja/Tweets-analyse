package bigdata;

import java.io.Serializable;

import java.util.List;
import java.util.ArrayList;

public class Entity implements Serializable{
    public List<HashTag> hashtags = new ArrayList<>();

    public List<HashTag> getHashtags() {
        return hashtags;
    }

    public String hashtagsToString() {
        String hashtagsString = "";
        for (HashTag ht : hashtags) {
            hashtagsString += ht.getText() + ",";
        }
        return hashtagsString;
    }

    public String toString() {
        return  "\n\thashtags: " + hashtags;
    }
}
