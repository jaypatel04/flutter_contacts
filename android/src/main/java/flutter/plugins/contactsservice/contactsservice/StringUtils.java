package flutter.plugins.contactsservice.contactsservice;

public class StringUtils {

    public static boolean equalsStrings(String first, String second) {
        return (first != null || second == null) && (first == null || second != null) && (first == null || second == null || first.equals(second));
    }

}
