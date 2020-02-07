package io.onurb.tools.qcsv;

public class Convert {

    public static float toFloat(String value) {
        String v = value.replace(",", ".");
        return Float.parseFloat(v);
    }

    public static void main(String[] args) {
        System.out.println(toFloat("100"));
    }

}
