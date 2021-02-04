package flutter.plugins.contactsservice.contactsservice;

public class StringUtils {

    public static boolean equalsStrings(String first, String second) {
        return (first != null || second == null) && (first == null || second != null) && (first == null || second == null || first.equals(second));
    }

    public static boolean isNullOrEmpty(String data) {
        return (data == null || data.isEmpty());
    }

    static boolean validAccountType(String accountType) {
        if (StringUtils.isNullOrEmpty(accountType)) {
            return true;
        }
        String accountTypeLowerCase = accountType.toLowerCase().trim();
        if (accountTypeLowerCase.contains("whatsapp") ||
                accountTypeLowerCase.contains("facebook") ||
                accountTypeLowerCase.contains("linkedin") ||
                accountTypeLowerCase.contains("telegram") ||
                accountTypeLowerCase.contains("twitter") ||
                accountTypeLowerCase.contains("signal")) {
            return false;
        }

        return true;
    }

}
