package bigdata;

import java.io.Serializable;
import java.lang.Comparable;

public class HashTag implements Serializable, Comparable {
    private String text;
    private int counter;
    private int id;
    private static int staticId;
    private String usersNames = "";

    public HashTag() {
        this.counter = 1;
        this.id = staticId;
        this.staticId += 1;
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

    public void setUsersrNames(String userName) {
        this.usersNames = userName;
    }

    public void mergeCounters(HashTag other) {
        this.counter += other.getCounter();
    }

    public void mergeUsersNames(HashTag other) {
        this.usersNames += (other.usersNames + ",");
    }

    public int compareTo(Object other) {
        if (this.counter < ((HashTag)other).getCounter())
            return -1;
        else if (this.counter == ((HashTag)other).counter)
            return 0;
        return 1;
    }

    public String toString() {
        return "text: " + text + ", counter:" + counter;
    }
}