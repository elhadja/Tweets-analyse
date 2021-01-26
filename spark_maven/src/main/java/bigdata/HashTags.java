package bigdata;


import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.lang.Comparable;
import java.util.ArrayList;
import java.util.List;

public class HashTags implements Serializable, Comparable {
    private int id;
    private static int staticId;
    private int counter;
    private String text;
    private List<HashTag> hashTags;
    private String usersNames = "";

    public HashTags(List<HashTag> hashTags) {
        this.id = staticId;
        this.staticId += 1;
        this.counter = 1;
        this.hashTags = new ArrayList<>();
        this.hashTags.addAll(hashTags);
        this.hashTags.forEach(hashTag -> {
            this.text += hashTag.getText() + ", ";
        });
    }

    public int getId() {
        return id;
    }

    public String getText() {
        return text;
    }

    public int getCounter() {
        return counter;
    }

    public String getUsersNames() {
        return usersNames;
    }

    public void setUsersNames(String userName) {
        this.usersNames = userName;
    }

    public void mergeCounters(HashTags other) {
        this.counter += other.getCounter();
    }

    public void mergeUsersNames(HashTags other) {
        this.usersNames += (other.usersNames + ",");
    }

    public int compareTo(Object other) {
        if (this.counter < ((HashTags)other).getCounter())
            return -1;
        else if (this.counter == ((HashTags)other).counter)
            return 0;
        return 1;
    }

    public String toString() {
        return "text: " + text + ", counter:" + counter;
    }
}