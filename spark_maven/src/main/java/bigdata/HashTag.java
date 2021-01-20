package bigdata;

import java.io.Serializable;

public class HashTag implements Serializable {
    private String text;

    public String getText() {
        return text;
    }

    public String toString() {
        return "text: " + text;
    }
}