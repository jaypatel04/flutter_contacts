package flutter.plugins.contactsservice.contactsservice;

import android.annotation.TargetApi;
import android.database.Cursor;
import android.os.Build;

import java.util.HashMap;

import static android.provider.ContactsContract.CommonDataKinds;
import static android.provider.ContactsContract.CommonDataKinds.StructuredPostal;
import static flutter.plugins.contactsservice.contactsservice.StringUtils.equalsStrings;

@TargetApi(Build.VERSION_CODES.ECLAIR)
public class PostalAddress {

    public String identifier, label, street, locality, city, postcode, region, country, formattedAddress;

    public PostalAddress(String identifier, String label, String street, String locality, String city, String postcode, String region,
                         String country, String formattedAddress) {
        this.identifier = identifier;
        this.label = label;
        this.street = street;
        this.locality = locality;
        this.city = city;
        this.postcode = postcode;
        this.region = region;
        this.country = country;
        this.formattedAddress = formattedAddress;
    }

    PostalAddress(Cursor cursor) {
        this.identifier = cursor.getString(cursor.getColumnIndex(StructuredPostal._ID));
        this.label = getLabel(cursor);
        this.street = cursor.getString(cursor.getColumnIndex(StructuredPostal.STREET));
        this.locality = cursor.getString(cursor.getColumnIndex(StructuredPostal.NEIGHBORHOOD));
        this.city = cursor.getString(cursor.getColumnIndex(StructuredPostal.CITY));
        this.postcode = cursor.getString(cursor.getColumnIndex(StructuredPostal.POSTCODE));
        this.region = cursor.getString(cursor.getColumnIndex(StructuredPostal.REGION));
        this.country = cursor.getString(cursor.getColumnIndex(StructuredPostal.COUNTRY));
        this.formattedAddress = cursor.getString(cursor.getColumnIndex(StructuredPostal.FORMATTED_ADDRESS));
    }

    HashMap<String, String> toMap() {
        HashMap<String, String> result = new HashMap<>();
        result.put("identifier", identifier);
        result.put("label", label);
        result.put("street", street);
        result.put("locality", locality);
        result.put("city", city);
        result.put("postcode", postcode);
        result.put("region", region);
        result.put("country", country);
        result.put("formattedAddress", formattedAddress);

        return result;
    }

    public static PostalAddress fromMap(HashMap<String, String> map) {
        return new PostalAddress(map.get("identifier"), map.get("label"), map.get("street"), map.get("locality"), map.get("city"), map.get(
                "postcode"), map.get("region"), map.get("country"), map.get("formattedAddress"));
    }

    private String getLabel(Cursor cursor) {
        switch (cursor.getInt(cursor.getColumnIndex(StructuredPostal.TYPE))) {
            case StructuredPostal.TYPE_HOME:
                return "home";
            case StructuredPostal.TYPE_WORK:
                return "work";
            case StructuredPostal.TYPE_OTHER:
                return "other";
            case StructuredPostal.TYPE_CUSTOM:
                final String label = cursor.getString(cursor.getColumnIndex(StructuredPostal.LABEL));
                return label != null ? label : "";
        }
        return "other";
    }

    public static int stringToPostalAddressType(String label) {
        if (label != null) {
            switch (label) {
                case "home":
                    return CommonDataKinds.StructuredPostal.TYPE_HOME;
                case "work":
                    return CommonDataKinds.StructuredPostal.TYPE_WORK;
                case "other":
                    return StructuredPostal.TYPE_OTHER;
                default:
                    return StructuredPostal.TYPE_CUSTOM;
            }
        }
        return StructuredPostal.TYPE_OTHER;
    }

    public boolean equalValues(PostalAddress item) {

        if (item == null) {
            return false;
        }
        if (!equalsStrings(label.toLowerCase(), item.label.toLowerCase())) {
            return false;
        }
        if (!equalsStrings(street, item.street)) {
            return false;
        }
        if (!equalsStrings(locality, item.locality)) {
            return false;
        }
        if (!equalsStrings(city, item.city)) {
            return false;
        }
        if (!equalsStrings(postcode, item.postcode)) {
            return false;
        }
        if (!equalsStrings(region, item.region)) {
            return false;
        }
        if (!equalsStrings(country, item.country)) {
            return false;
        }
        if (!equalsStrings(formattedAddress, item.formattedAddress)) {
            return false;
        }

        return true;
    }

    public boolean equals(PostalAddress item) {
        if (!equalValues(item)) {
            return false;
        }
        if (!equalsStrings(identifier, item.identifier)) {
            return false;
        }
        return true;
    }


    public String toString() {
        String finalString = "";
        if (this.identifier != null) {
            finalString = this.identifier;
        }
        if (this.street != null) {
            if (!finalString.isEmpty()) {
                finalString += ", " + this.street;
            } else {
                finalString += this.street;
            }
        }
        if (this.locality != null) {
            if (!finalString.isEmpty()) {
                finalString += ", " + this.locality;
            } else {
                finalString += this.locality;
            }
        }
        if (this.city != null) {
            if (!finalString.isEmpty()) {
                finalString += ", " + this.city;
            } else {
                finalString += this.city;
            }
        }
        if (this.region != null) {
            if (!finalString.isEmpty()) {
                finalString += ", " + this.region;
            } else {
                finalString += this.region;
            }
        }
        if (this.postcode != null) {
            if (!finalString.isEmpty()) {
                finalString += " " + this.postcode;
            } else {
                finalString += this.postcode;
            }
        }
        if (this.country != null) {
            if (!finalString.isEmpty()) {
                finalString += ", " + this.country;
            } else {
                finalString += this.country;
            }
        }
        return finalString;
    }

}
