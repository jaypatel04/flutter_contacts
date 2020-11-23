package flutter.plugins.contactsservice.contactsservice;

import android.annotation.TargetApi;
import android.content.ContentProviderOperation;
import android.content.ContentProviderResult;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Build;
import android.provider.BaseColumns;
import android.provider.ContactsContract;
import android.text.TextUtils;
import android.util.Log;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.flutter.embedding.engine.plugins.FlutterPlugin;
import io.flutter.embedding.engine.plugins.activity.ActivityAware;
import io.flutter.embedding.engine.plugins.activity.ActivityPluginBinding;
import io.flutter.plugin.common.BinaryMessenger;
import io.flutter.plugin.common.MethodCall;
import io.flutter.plugin.common.MethodChannel;
import io.flutter.plugin.common.MethodChannel.MethodCallHandler;
import io.flutter.plugin.common.MethodChannel.Result;
import io.flutter.plugin.common.PluginRegistry;
import io.flutter.plugin.common.PluginRegistry.Registrar;

import static android.app.Activity.RESULT_CANCELED;
import static android.provider.BaseColumns._ID;
import static android.provider.ContactsContract.CommonDataKinds;
import static android.provider.ContactsContract.CommonDataKinds.Email;
import static android.provider.ContactsContract.CommonDataKinds.Organization;
import static android.provider.ContactsContract.CommonDataKinds.Phone;
import static android.provider.ContactsContract.CommonDataKinds.StructuredName;
import static android.provider.ContactsContract.CommonDataKinds.StructuredPostal;
import static flutter.plugins.contactsservice.contactsservice.StringUtils.equalsStrings;

@TargetApi(Build.VERSION_CODES.ECLAIR)
public class ContactsServicePlugin implements MethodCallHandler, FlutterPlugin, ActivityAware {

    private static final int FORM_OPERATION_CANCELED = 1;
    private static final int FORM_COULD_NOT_BE_OPEN = 2;

    private static final String LOG_TAG = "flutter_contacts";
    private ContentResolver contentResolver;
    private MethodChannel methodChannel;
    private BaseContactsServiceDelegate delegate;

    private static final String getContactsMethod = "getContacts";
    private static final String getContactsByIdentifiersMethod = "getContactsByIdentifiers";
    private static final String getIdentifiersMethod = "getIdentifiers";
    private static final String getContactsSummaryMethod = "getContactsSummary";
    private static final String getContactsForPhoneMethod = "getContactsForPhone";
    private static final String getContactsForEmailMethod = "getContactsForEmail";
    private static final String deleteContactsByIdentifiersMethod = "deleteContactsByIdentifiers";
    private static final String getContactsLookupKeysMethod = "getContactsLookupKeys";

    private static final String addContactMethod = "addContact";
    private static final String deleteContactMethod = "deleteContact";
    private static final String updateContactMethod = "updateContact";
    private static final String openContactFormMethod = "openContactForm";
    private static final String openExistingContactMethod = "openExistingContact";
    private static final String openDeviceContactPickerMethod = "openDeviceContactPicker";
    private static final String getAvatarMethod = "getAvatar";
    private static final String addContactWithReturnIdentifierMethod = "addContactWithReturnIdentifier";


    private final ExecutorService executor =
            new ThreadPoolExecutor(0, 10, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1000));

    private void initDelegateWithRegister(Registrar registrar) {
        this.delegate = new ContactServiceDelegateOld(registrar);
    }

    public static void registerWith(Registrar registrar) {
        ContactsServicePlugin instance = new ContactsServicePlugin();
        instance.initInstance(registrar.messenger(), registrar.context());
        instance.initDelegateWithRegister(registrar);
    }

    private void initInstance(BinaryMessenger messenger, Context context) {
        methodChannel = new MethodChannel(messenger, "github.com/clovisnicolas/flutter_contacts");
        methodChannel.setMethodCallHandler(this);
        this.contentResolver = context.getContentResolver();
    }

    @Override
    public void onAttachedToEngine(FlutterPluginBinding binding) {
        initInstance(binding.getBinaryMessenger(), binding.getApplicationContext());
        this.delegate = new ContactServiceDelegate(binding.getApplicationContext());
    }

    @Override
    public void onDetachedFromEngine(FlutterPluginBinding binding) {
        methodChannel.setMethodCallHandler(null);
        methodChannel = null;
        contentResolver = null;
        this.delegate = null;
    }

    @Override
    public void onMethodCall(MethodCall call, Result result) {
        switch (call.method) {
            case getContactsByIdentifiersMethod:
            case getIdentifiersMethod:
            case getContactsSummaryMethod:
            case getContactsMethod: {
                this.getContacts(call.method, (String) call.argument("query"), (boolean) call.argument("withThumbnails"), (boolean) call.argument(
                        "photoHighResolution"), (boolean) call.argument("orderByGivenName"), (String) call.argument("identifiers"), result);
                break;
            }
            case getAvatarMethod: {
                final Contact contact = Contact.fromMap((HashMap) call.argument("contact"));
                this.getAvatar(contact, (boolean) call.argument("photoHighResolution"), result);
                break;
            }
            case addContactMethod: {
                final Contact contact = Contact.fromMap((HashMap) call.arguments);
                if (this.addContact(contact)) {
                    result.success(null);
                } else {
                    result.error(null, "Failed to add the contact", null);
                }
                break;
            }
            case addContactWithReturnIdentifierMethod: {
                final Contact contact = Contact.fromMap((HashMap) call.arguments);
                String identifier = this.addContactWithReturnIdentifier(contact);
                if (!identifier.isEmpty()) {
                    ArrayList<HashMap> maps = new ArrayList();
                    HashMap<String, String> map = new HashMap<>();
                    map.put("identifier", identifier);
                    maps.add(map);
                    result.success(maps);
                } else {
                    result.error(null, "Failed to add the contact", null);
                }
                break;
            }
            case deleteContactsByIdentifiersMethod: {
                String identifierString = (String) call.argument("identifiers");
                if (identifierString == null) {
                    result.success(null);
                } else {
                    List<String> identifiersList = Arrays.asList(identifierString.split("\\|"));
                    if (this.deleteContactsByIdentifiers(identifiersList)) {
                        result.success(null);
                    } else {
                        result.error(null, "Failed to delete the contacts, make sure they have valid identifiers", null);
                    }
                }
            }
            case deleteContactMethod: {
                final Contact contact = Contact.fromMap((HashMap) call.arguments);
                if (this.deleteContact(contact)) {
                    result.success(null);
                } else {
                    result.error(null, "Failed to delete the contact, make sure it has a valid identifier", null);
                }
                break;
            }
            case updateContactMethod: {
                final Contact contact = Contact.fromMap((HashMap) call.arguments);
                if (this.updateContact(contact)) {
                    result.success(null);
                } else {
                    result.error(null, "Failed to update the contact, make sure it has a valid identifier", null);
                }
                break;
            }
            case getContactsLookupKeysMethod: {
                new GetLookupKeysTask(result).executeOnExecutor(executor);
                break;
            }
//            case openExistingContactMethod: {
//                final Contact contact = Contact.fromMap((HashMap) call.argument("contact"));
//                if (delegate != null) {
//                    delegate.setResult(result);
//                    delegate.openExistingContact(contact);
//                } else {
//                    result.success(FORM_COULD_NOT_BE_OPEN);
//                }
//                break;
//            }
//            case openContactFormMethod: {
//                if (delegate != null) {
//                    delegate.setResult(result);
//                    delegate.openContactForm();
//                } else {
//                    result.success(FORM_COULD_NOT_BE_OPEN);
//                }
//                break;
//            }
//            case openDeviceContactPickerMethod: {
//                openDeviceContactPicker(result);
//                break;
//            }
            default: {
                result.notImplemented();
                break;
            }
        }
    }

    private static final String[] PROJECTION = {
            ContactsContract.Data.LOOKUP_KEY,
            ContactsContract.Data.CONTACT_ID,
            ContactsContract.Profile.DISPLAY_NAME,
            ContactsContract.Contacts.Data.MIMETYPE,
            ContactsContract.RawContacts.ACCOUNT_TYPE,
            ContactsContract.RawContacts.ACCOUNT_NAME,
            StructuredName.DISPLAY_NAME,
            StructuredName.GIVEN_NAME,
            StructuredName.MIDDLE_NAME,
            StructuredName.FAMILY_NAME,
            StructuredName.PREFIX,
            StructuredName.SUFFIX,
            StructuredName.PHONETIC_GIVEN_NAME,
            StructuredName.PHONETIC_MIDDLE_NAME,
            StructuredName.PHONETIC_FAMILY_NAME,
            StructuredName.PHONETIC_NAME,
            CommonDataKinds.Note.NOTE,
            CommonDataKinds.Nickname.NAME,
            BaseColumns._ID,
            Phone.NUMBER,
            Phone.TYPE,
            Phone.LABEL,
            Email.DATA,
            Email.ADDRESS,
            Email.TYPE,
            Email.LABEL,
            Organization.COMPANY,
            Organization.TITLE,
            Organization.DEPARTMENT,
            StructuredPostal.FORMATTED_ADDRESS,
            StructuredPostal.TYPE,
            StructuredPostal.LABEL,
            StructuredPostal.STREET,
            StructuredPostal.POBOX,
            StructuredPostal.NEIGHBORHOOD,
            StructuredPostal.CITY,
            StructuredPostal.REGION,
            StructuredPostal.POSTCODE,
            StructuredPostal.COUNTRY,
            CommonDataKinds.Im.DATA,
            CommonDataKinds.Im.LABEL,
            CommonDataKinds.Im.TYPE,
            CommonDataKinds.Im.PROTOCOL,
            CommonDataKinds.Im.CUSTOM_PROTOCOL,
            CommonDataKinds.Relation.NAME,
            CommonDataKinds.Relation.LABEL,
            CommonDataKinds.Relation.TYPE,
            CommonDataKinds.Event.START_DATE,
            CommonDataKinds.Event.LABEL,
            CommonDataKinds.Event.TYPE,
            CommonDataKinds.Website.URL,
            ContactsContract.Data.DATA1,
    };

    private static final String[] SUMMARY_PROJECTION = {
            ContactsContract.Data.LOOKUP_KEY,
            ContactsContract.Contacts.DISPLAY_NAME,
    };

    private static final String ORDER_BY_FIELD = (ContactsContract.Contacts.DISPLAY_NAME + " COLLATE NOCASE ASC");

    @TargetApi(Build.VERSION_CODES.ECLAIR)
    private void getContacts(String callMethod, String query, boolean withThumbnails, boolean photoHighResolution, boolean orderByGivenName,
                             String identifiers, Result result) {
        List<String> identifiersList = null;
        if (identifiers != null) {
            identifiersList = Arrays.asList(identifiers.split("\\|"));
        }
        new GetContactsTask(callMethod, result, withThumbnails, photoHighResolution, orderByGivenName, identifiersList).executeOnExecutor(executor, query, false);
    }

    private void getContactsForPhone(String callMethod, String phone, boolean withThumbnails, boolean photoHighResolution, boolean orderByGivenName, Result result) {
        new GetContactsTask(callMethod, result, withThumbnails, photoHighResolution, orderByGivenName, null).executeOnExecutor(executor, phone, true);
    }

    @Override
    public void onAttachedToActivity(ActivityPluginBinding binding) {
        if (delegate instanceof ContactServiceDelegate) {
            ((ContactServiceDelegate) delegate).bindToActivity(binding);
        }
    }

    @Override
    public void onDetachedFromActivityForConfigChanges() {
        if (delegate instanceof ContactServiceDelegate) {
            ((ContactServiceDelegate) delegate).unbindActivity();
        }
    }

    @Override
    public void onReattachedToActivityForConfigChanges(ActivityPluginBinding binding) {
        if (delegate instanceof ContactServiceDelegate) {
            ((ContactServiceDelegate) delegate).bindToActivity(binding);
        }
    }

    @Override
    public void onDetachedFromActivity() {
        if (delegate instanceof ContactServiceDelegate) {
            ((ContactServiceDelegate) delegate).unbindActivity();
        }
    }

    private class BaseContactsServiceDelegate implements PluginRegistry.ActivityResultListener {
        private static final int REQUEST_OPEN_CONTACT_FORM = 52941;
        private static final int REQUEST_OPEN_EXISTING_CONTACT = 52942;
        private static final int REQUEST_OPEN_CONTACT_PICKER = 52943;
        private Result result;

        void setResult(Result result) {
            this.result = result;
        }

        void finishWithResult(Object result) {
            if (this.result != null) {
                this.result.success(result);
                this.result = null;
            }
        }

        @Override
        public boolean onActivityResult(int requestCode, int resultCode, Intent intent) {
            if (requestCode == REQUEST_OPEN_EXISTING_CONTACT || requestCode == REQUEST_OPEN_CONTACT_FORM) {
                try {
                    Uri ur = intent.getData();
                    finishWithResult(getContactByIdentifier(ur.getLastPathSegment()));
                } catch (NullPointerException e) {
                    finishWithResult(FORM_OPERATION_CANCELED);
                }
                return true;
            }

            if (requestCode == REQUEST_OPEN_CONTACT_PICKER) {
                if (resultCode == RESULT_CANCELED) {
                    finishWithResult(FORM_OPERATION_CANCELED);
                    return true;
                }
                Uri contactUri = intent.getData();
                Cursor cursor = contentResolver.query(contactUri, null, null, null, null);
                if (cursor.moveToFirst()) {
                    String id = contactUri.getLastPathSegment();
                    getContacts(openDeviceContactPickerMethod, id, false, false, false, null, this.result);
                } else {
                    Log.e(LOG_TAG, "onActivityResult - cursor.moveToFirst() returns false");
                    finishWithResult(FORM_OPERATION_CANCELED);
                }
                cursor.close();
                return true;
            }

            finishWithResult(FORM_COULD_NOT_BE_OPEN);
            return false;
        }

        void openExistingContact(Contact contact) {
            String identifier = contact.identifier;
            try {
                HashMap contactMapFromDevice = getContactByIdentifier(identifier);
                // Contact existence check
                if (contactMapFromDevice != null) {
                    Uri uri = Uri.withAppendedPath(ContactsContract.Contacts.CONTENT_URI, identifier);
                    Intent intent = new Intent(Intent.ACTION_EDIT);
                    intent.setDataAndType(uri, ContactsContract.Contacts.CONTENT_ITEM_TYPE);
                    intent.putExtra("finishActivityOnSaveCompleted", true);
                    startIntent(intent, REQUEST_OPEN_EXISTING_CONTACT);
                } else {
                    finishWithResult(FORM_COULD_NOT_BE_OPEN);
                }
            } catch (Exception e) {
                finishWithResult(FORM_COULD_NOT_BE_OPEN);
            }
        }

        void openContactForm() {
            try {
                Intent intent = new Intent(Intent.ACTION_INSERT, ContactsContract.Contacts.CONTENT_URI);
                intent.putExtra("finishActivityOnSaveCompleted", true);
                startIntent(intent, REQUEST_OPEN_CONTACT_FORM);
            } catch (Exception e) {
            }
        }

        void openContactPicker() {
            Intent intent = new Intent(Intent.ACTION_PICK);
            intent.setType(ContactsContract.Contacts.CONTENT_TYPE);
            startIntent(intent, REQUEST_OPEN_CONTACT_PICKER);
        }

        void startIntent(Intent intent, int request) {
        }

        HashMap getContactByIdentifier(String identifier) {
            ArrayList<Contact> matchingContacts;
            {
                Cursor cursor = contentResolver.query(
                        ContactsContract.Data.CONTENT_URI, PROJECTION,
                        ContactsContract.RawContacts.CONTACT_ID + " = ?",
                        new String[]{identifier},
                        null
                );
                try {
                    matchingContacts = getContactsFrom(cursor);
                } finally {
                    if (cursor != null) {
                        cursor.close();
                    }
                }
            }
            if (matchingContacts.size() > 0) {
                return matchingContacts.iterator().next().toMap();
            }
            return null;
        }
    }

    private void openDeviceContactPicker(Result result) {
        if (delegate != null) {
            delegate.setResult(result);
            delegate.openContactPicker();
        } else {
            result.success(FORM_COULD_NOT_BE_OPEN);
        }
    }

    private class ContactServiceDelegateOld extends BaseContactsServiceDelegate {
        private final PluginRegistry.Registrar registrar;

        ContactServiceDelegateOld(PluginRegistry.Registrar registrar) {
            this.registrar = registrar;
            registrar.addActivityResultListener(this);
        }

        @Override
        void startIntent(Intent intent, int request) {
            if (registrar.activity() != null) {
                registrar.activity().startActivityForResult(intent, request);
            } else {
                registrar.context().startActivity(intent);
            }
        }
    }

    private class ContactServiceDelegate extends BaseContactsServiceDelegate {
        private final Context context;
        private ActivityPluginBinding activityPluginBinding;

        ContactServiceDelegate(Context context) {
            this.context = context;
        }

        void bindToActivity(ActivityPluginBinding activityPluginBinding) {
            this.activityPluginBinding = activityPluginBinding;
            this.activityPluginBinding.addActivityResultListener(this);
        }

        void unbindActivity() {
            this.activityPluginBinding.removeActivityResultListener(this);
            this.activityPluginBinding = null;
        }

        @Override
        void startIntent(Intent intent, int request) {
            if (this.activityPluginBinding != null) {
                if (intent.resolveActivity(context.getPackageManager()) != null) {
                    activityPluginBinding.getActivity().startActivityForResult(intent, request);
                } else {
                    finishWithResult(FORM_COULD_NOT_BE_OPEN);
                }
            } else {
                context.startActivity(intent);
            }
        }
    }

    @TargetApi(Build.VERSION_CODES.CUPCAKE)
    private class GetContactsTask extends AsyncTask<Object, Void, ArrayList<HashMap>> {

        private String callMethod;
        private Result getContactResult;
        private boolean withThumbnails;
        private boolean photoHighResolution;
        private boolean orderByGivenName;
        private List<String> identifiers;

        public GetContactsTask(String callMethod, MethodChannel.Result result, boolean withThumbnails, boolean photoHighResolution,
                               boolean orderByGivenName, List<String> identifiers) {
            this.callMethod = callMethod;
            this.getContactResult = result;
            this.withThumbnails = withThumbnails;
            this.photoHighResolution = photoHighResolution;
            this.orderByGivenName = orderByGivenName;
            this.identifiers = identifiers;
        }

        @TargetApi(Build.VERSION_CODES.ECLAIR)
        protected ArrayList<HashMap> doInBackground(Object... params) {
            ArrayList<Contact> contacts;
            switch (callMethod) {
                case getContactsByIdentifiersMethod:
                    contacts = getContactsFrom(getCursorForContactIdentifiers(identifiers, orderByGivenName));
                    break;
                case getContactsMethod:
                    contacts = getContactsFrom(getCursor(null, orderByGivenName));
                    break;
                case getContactsSummaryMethod:
                    contacts = getContactsSummary(orderByGivenName);
                    break;
                case getIdentifiersMethod:
                    ArrayList<String> contactList = getContactIdentifiersFrom(getCursorForIdentifiers(orderByGivenName));
                    ArrayList<HashMap> mapList = new ArrayList<>();
                    HashMap map = new HashMap();
                    map.put("identifiers", contactList);
                    mapList.add(map);
                    return mapList;
                default:
                    return null;
            }

            if (withThumbnails) {
                for (Contact c : contacts) {
                    final byte[] avatar = loadContactPhotoHighRes(
                            c.identifier, photoHighResolution, contentResolver);
                    if (avatar != null) {
                        c.avatar = avatar;
                    } else {
                        // To stay backwards-compatible, return an empty byte array rather than `null`.
                        c.avatar = new byte[0];
                    }
                }
            }

            //Transform the list of contacts to a list of Map

            ArrayList<HashMap> contactMaps = new ArrayList<>();

            if (callMethod.equalsIgnoreCase(getContactsSummaryMethod)) {
                for (Contact c : contacts) {
                    contactMaps.add(c.toSummaryMap());
                }
            } else {
                for (Contact c : contacts) {
                    contactMaps.add(c.toMap());
                }
            }

            return contactMaps;
        }

        protected void onPostExecute(ArrayList<HashMap> result) {
            if (result == null) {
                getContactResult.notImplemented();
            } else {
                getContactResult.success(result);
            }
        }
    }

    @TargetApi(Build.VERSION_CODES.CUPCAKE)
    private class GetLookupKeysTask extends AsyncTask<Object, Void, HashMap> {

        private Result getContactResult;

        public GetLookupKeysTask(MethodChannel.Result result) {
            this.getContactResult = result;
        }

        @TargetApi(Build.VERSION_CODES.ECLAIR)
        protected HashMap doInBackground(Object... params) {
            HashMap<String, String> lookupKeysMap = new HashMap<>();

            Cursor cursor = contentResolver.query(ContactsContract.Contacts.CONTENT_URI, new String[]{_ID, ContactsContract.Contacts.LOOKUP_KEY}, null, null, null);

            if (cursor != null && cursor.getCount() > 0) {
                cursor.moveToPosition(-1);
                while (cursor.moveToNext()) {
                    lookupKeysMap.put(cursor.getString(cursor.getColumnIndex(_ID)),
                            cursor.getString(cursor.getColumnIndex(ContactsContract.Contacts.LOOKUP_KEY)));
                }
            }

            return lookupKeysMap;
        }

        protected void onPostExecute(HashMap result) {
            if (result == null) {
                getContactResult.notImplemented();
            } else {
                getContactResult.success(result);
            }
        }
    }

    private Cursor getCursor(String lookupKey, boolean orderByGivenName) {

        if (lookupKey == null) {
            //retrieve all contacts
            if (orderByGivenName) {
                return contentResolver.query(ContactsContract.Data.CONTENT_URI, PROJECTION, null, null, ORDER_BY_FIELD);
            } else {
                return contentResolver.query(ContactsContract.Data.CONTENT_URI, PROJECTION, null, null, null);
            }
        } else {
            //get contact with lookup key

            String contactId = getContactIdFromLookupKey(lookupKey);

            if (contactId != null) {
                String selection = ContactsContract.Data.CONTACT_ID + " = ? ";
                ArrayList<String> selectionArgs = new ArrayList<>();
                selectionArgs.add(contactId);

                return contentResolver.query(ContactsContract.Data.CONTENT_URI, PROJECTION, selection,
                        selectionArgs.toArray(new String[selectionArgs.size()]), null);
            }
        }
        return null;
    }

    private Cursor getCursorForContactIdentifiers(List<String> lookupKeyList, boolean orderByGivenName) {
        List<String> contactIdList = new ArrayList<>();
        if (lookupKeyList != null && lookupKeyList.size() > 0) {
            for (String lookupKey : lookupKeyList) {
                String contactId = getContactIdFromLookupKey(lookupKey);
                if (contactId != null) {
                    contactIdList.add(contactId);
                }
            }
        }

        if (contactIdList.size() == 0) {
            return null;
        }

        String selectionString = "";

        for (String i : contactIdList) {
            selectionString += "?,";
        }

        String selection = ContactsContract.Data.CONTACT_ID + " IN (" + selectionString.substring(0, selectionString.length() - 1) + ")";

        if (orderByGivenName) {
            return contentResolver.query(ContactsContract.Data.CONTENT_URI, PROJECTION, selection,
                    contactIdList.toArray(new String[contactIdList.size()]), ORDER_BY_FIELD);
        }
        return contentResolver.query(ContactsContract.Data.CONTENT_URI, PROJECTION, selection,
                contactIdList.toArray(new String[contactIdList.size()]), null);
    }

    private Cursor getCursorForPhone(String phone, boolean orderByGivenName) {
        if (phone.isEmpty()) {
            return null;
        }

        Uri uri = Uri.withAppendedPath(ContactsContract.PhoneLookup.CONTENT_FILTER_URI, Uri.encode(phone));
        String[] projection = new String[]{BaseColumns._ID};

        ArrayList<String> contactIds = new ArrayList<>();
        Cursor phoneCursor = contentResolver.query(uri, projection, null, null, null);
        while (phoneCursor != null && phoneCursor.moveToNext()) {
            contactIds.add(phoneCursor.getString(phoneCursor.getColumnIndex(BaseColumns._ID)));
        }
        if (phoneCursor != null) {
            phoneCursor.close();
        }

        if (!contactIds.isEmpty()) {
            String contactIdsListString = contactIds.toString().replace("[", "(").replace("]", ")");
            String contactSelection = ContactsContract.Data.CONTACT_ID + " IN " + contactIdsListString;
            if (orderByGivenName) {
                return contentResolver.query(ContactsContract.Data.CONTENT_URI, PROJECTION, contactSelection, null, ORDER_BY_FIELD);

            }
            return contentResolver.query(ContactsContract.Data.CONTENT_URI, PROJECTION, contactSelection, null, null);
        }

        return null;
    }

    private ArrayList<Contact> getContactsSummary(boolean orderByGivenName) {
        Cursor cursor = null;
        if (orderByGivenName) {
            cursor = contentResolver.query(ContactsContract.Contacts.CONTENT_URI, SUMMARY_PROJECTION, null, null, ORDER_BY_FIELD);
        } else {
            cursor = contentResolver.query(ContactsContract.Contacts.CONTENT_URI, SUMMARY_PROJECTION, null, null, null);
        }

        if (cursor != null && cursor.getCount() > 0) {
            ArrayList<Contact> list = new ArrayList<>();
            while (cursor != null && cursor.moveToNext()) {
                String lookupKey = cursor.getString(cursor.getColumnIndex(ContactsContract.Contacts.LOOKUP_KEY));
                Contact contact = new Contact(lookupKey);
                contact.displayName = cursor.getString(cursor.getColumnIndex(ContactsContract.Contacts.DISPLAY_NAME));
                list.add(contact);
            }
            if (cursor != null) {
                cursor.close();
            }
            return list;
        }
        return new ArrayList<Contact>();
    }

    private Cursor getCursorForIdentifiers(boolean orderByGivenName) {
        String[] projection = new String[]{ContactsContract.Contacts.LOOKUP_KEY};
        if (orderByGivenName) {
            return contentResolver.query(ContactsContract.Contacts.CONTENT_URI, projection, null, null, ORDER_BY_FIELD);
        }
        return contentResolver.query(ContactsContract.Contacts.CONTENT_URI, projection, null, null, null);
    }

    private ArrayList<Contact> getContactsFrom(Cursor cursor) {
        return getContactsFrom(cursor, false);
    }

    /**
     * Builds the list of contacts from the cursor
     *
     * @param cursor
     * @return the list of contacts
     */
    private ArrayList<Contact> getContactsFrom(Cursor cursor, boolean summaryFields) {
        HashMap<String, Contact> map = new LinkedHashMap<>();

        if (cursor != null && cursor.getCount() > 0) {
            cursor.moveToPosition(-1);
        } else {
            return new ArrayList<>(map.values());
        }

        while (cursor != null && cursor.moveToNext()) {
            int columnIndex = cursor.getColumnIndex(ContactsContract.Data.LOOKUP_KEY);
            String lookupKey = cursor.getString(columnIndex);

            if (!map.containsKey(lookupKey)) {
                map.put(lookupKey, new Contact(lookupKey));
            }
            Contact contact = map.get(lookupKey);

            contact.identifier = lookupKey;
            contact.displayName = cursor.getString(cursor.getColumnIndex(ContactsContract.Contacts.DISPLAY_NAME));
            String mimeType = cursor.getString(cursor.getColumnIndex(ContactsContract.Data.MIMETYPE));

            //NAMES
            if (mimeType.equals(CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE)) {
                contact.givenName = cursor.getString(cursor.getColumnIndex(StructuredName.GIVEN_NAME));
                contact.middleName = cursor.getString(cursor.getColumnIndex(StructuredName.MIDDLE_NAME));
                contact.familyName = cursor.getString(cursor.getColumnIndex(StructuredName.FAMILY_NAME));
                contact.prefix = cursor.getString(cursor.getColumnIndex(StructuredName.PREFIX));
                contact.suffix = cursor.getString(cursor.getColumnIndex(StructuredName.SUFFIX));
            }

            if (!summaryFields) {
                contact.androidAccountType = cursor.getString(cursor.getColumnIndex(ContactsContract.RawContacts.ACCOUNT_TYPE));
                contact.androidAccountName = cursor.getString(cursor.getColumnIndex(ContactsContract.RawContacts.ACCOUNT_NAME));

                if (mimeType.equals(CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE)) {
                    contact.phoneticGivenName = cursor.getString(cursor.getColumnIndex(StructuredName.PHONETIC_GIVEN_NAME));
                    contact.phoneticMiddleName = cursor.getString(cursor.getColumnIndex(StructuredName.PHONETIC_MIDDLE_NAME));
                    contact.phoneticFamilyName = cursor.getString(cursor.getColumnIndex(StructuredName.PHONETIC_FAMILY_NAME));
                    contact.phoneticName = cursor.getString(cursor.getColumnIndex(StructuredName.PHONETIC_NAME));
                }
                //NICK NAME
                if (mimeType.equals(CommonDataKinds.Nickname.CONTENT_ITEM_TYPE)) {
                    contact.nickname = cursor.getString(cursor.getColumnIndex(CommonDataKinds.Nickname.NAME));
                }
                // SIP
                else if (mimeType.equals(CommonDataKinds.SipAddress.CONTENT_ITEM_TYPE)) {
                    contact.sip = cursor.getString(cursor.getColumnIndex(CommonDataKinds.SipAddress.SIP_ADDRESS));
                }
                // NOTE
                else if (mimeType.equals(CommonDataKinds.Note.CONTENT_ITEM_TYPE)) {
                    contact.note = cursor.getString(cursor.getColumnIndex(CommonDataKinds.Note.NOTE));
                }
                //PHONES
                else if (mimeType.equals(CommonDataKinds.Phone.CONTENT_ITEM_TYPE)) {
                    String phoneNumber = cursor.getString(cursor.getColumnIndex(Phone.NUMBER));
                    if (!TextUtils.isEmpty(phoneNumber)) {
                        int type = cursor.getInt(cursor.getColumnIndex(Phone.TYPE));
                        String label = Item.getPhoneLabel(type, cursor);
                        contact.phones.add(new Item(cursor.getString(cursor.getColumnIndex(BaseColumns._ID)), label, phoneNumber));
                    }
                }
                //MAILS
                else if (mimeType.equals(CommonDataKinds.Email.CONTENT_ITEM_TYPE)) {
                    String email = cursor.getString(cursor.getColumnIndex(Email.ADDRESS));
                    int type = cursor.getInt(cursor.getColumnIndex(Email.TYPE));
                    if (!TextUtils.isEmpty(email)) {
                        contact.emails.add(new Item(cursor.getString(cursor.getColumnIndex(BaseColumns._ID)), Item.getEmailLabel(type, cursor), email));
                    }
                }
                //ORG
                else if (mimeType.equals(CommonDataKinds.Organization.CONTENT_ITEM_TYPE)) {
                    contact.company = cursor.getString(cursor.getColumnIndex(Organization.COMPANY));
                    contact.jobTitle = cursor.getString(cursor.getColumnIndex(Organization.TITLE));
                    contact.department = cursor.getString(cursor.getColumnIndex(Organization.DEPARTMENT));
                }
                //ADDRESSES
                else if (mimeType.equals(CommonDataKinds.StructuredPostal.CONTENT_ITEM_TYPE)) {
                    contact.postalAddresses.add(new PostalAddress(cursor));
                }
                // BIRTHDAY/EVENTS(DATES)
                else if (mimeType.equals(CommonDataKinds.Event.CONTENT_ITEM_TYPE)) {
                    String date = cursor.getString(cursor.getColumnIndex(CommonDataKinds.Event.START_DATE));
                    int eventType = cursor.getInt(cursor.getColumnIndex(CommonDataKinds.Event.TYPE));
                    if (eventType == CommonDataKinds.Event.TYPE_BIRTHDAY) {
                        contact.birthday = date;
                    } else {
                        contact.dates.add(new Item(cursor.getString(cursor.getColumnIndex(BaseColumns._ID)), Item.getDatesLabel(eventType, cursor),
                                date));
                    }
                }
                //INSTANT MESSAGE ADDRESSES / Im
                else if (mimeType.equals(CommonDataKinds.Im.CONTENT_ITEM_TYPE)) {
                    String im = cursor.getString(cursor.getColumnIndex(CommonDataKinds.Im.DATA));
                    int protocol = cursor.getInt(cursor.getColumnIndex(CommonDataKinds.Im.PROTOCOL));
                    if (!TextUtils.isEmpty(im)) {
                        contact.instantMessageAddresses.add(new Item(cursor.getString(cursor.getColumnIndex(BaseColumns._ID)),
                                Item.getInstantMessageAddressLabel(protocol, cursor), im));
                    }
                }
                //RELATIONS
                else if (mimeType.equals(CommonDataKinds.Relation.CONTENT_ITEM_TYPE)) {
                    String relation = cursor.getString(cursor.getColumnIndex(CommonDataKinds.Relation.NAME));
                    int type = cursor.getInt(cursor.getColumnIndex(CommonDataKinds.Relation.TYPE));
                    if (!TextUtils.isEmpty(relation)) {
                        contact.relations.add(new Item(cursor.getString(cursor.getColumnIndex(BaseColumns._ID)), Item.getRelationLabel(type,
                                cursor), relation));
                    }
                }
                //WEBSITES
                else if (mimeType.equals(CommonDataKinds.Website.CONTENT_ITEM_TYPE)) {
                    String url = cursor.getString(cursor.getColumnIndex(CommonDataKinds.Website.URL));
                    int type = cursor.getInt(cursor.getColumnIndex(CommonDataKinds.Website.TYPE));
                    if (!TextUtils.isEmpty(url)) {
                        contact.websites.add(new Item(cursor.getString(cursor.getColumnIndex(BaseColumns._ID)), Item.getWebsiteLabel(type,
                                cursor), url));
                    }
                }
                //LABELS
                else if (mimeType.equals(CommonDataKinds.GroupMembership.CONTENT_ITEM_TYPE)) {
                    String groupId = cursor.getString(cursor.getColumnIndex(CommonDataKinds.GroupMembership.DATA1));
                    if (!TextUtils.isEmpty(groupId)) {
                        ArrayList<String> groupTitle = getLabelGroupTitle(groupId);
                        if (groupTitle.size() > 0) {
                            contact.labels.addAll(groupTitle);
                        }
                    }
                }
            }
        }

        if (cursor != null) {
            cursor.close();
        }

        return new ArrayList<>(map.values());
    }

    private ArrayList<String> getContactIdentifiersFrom(Cursor cursor) {
        ArrayList<String> result = new ArrayList<>();
        while (cursor != null && cursor.moveToNext()) {
            int columnIndex = cursor.getColumnIndex(ContactsContract.Contacts.LOOKUP_KEY);
            result.add(cursor.getString(columnIndex));
        }

        if (cursor != null) {
            cursor.close();
        }
        return result;
    }

    private void setAvatarDataForContactIfAvailable(Contact contact) {
        Uri contactUri = ContentUris.withAppendedId(ContactsContract.Contacts.CONTENT_URI, Integer.parseInt(contact.identifier));
        Uri photoUri = Uri.withAppendedPath(contactUri, ContactsContract.Contacts.Photo.CONTENT_DIRECTORY);
        Cursor avatarCursor = contentResolver.query(photoUri,
                new String[]{ContactsContract.Contacts.Photo.PHOTO}, null, null, null);
        if (avatarCursor != null && avatarCursor.moveToFirst()) {
            byte[] avatar = avatarCursor.getBlob(0);
            contact.avatar = avatar;
        }
        if (avatarCursor != null) {
            avatarCursor.close();
        }
    }

    private void getAvatar(final Contact contact, final boolean highRes,
                           final Result result) {
        new GetAvatarsTask(contact, highRes, contentResolver, result).executeOnExecutor(this.executor);
    }

    private static class GetAvatarsTask extends AsyncTask<Void, Void, byte[]> {
        final Contact contact;
        final boolean highRes;
        final ContentResolver contentResolver;
        final Result result;

        GetAvatarsTask(final Contact contact, final boolean highRes,
                       final ContentResolver contentResolver, final Result result) {
            this.contact = contact;
            this.highRes = highRes;
            this.contentResolver = contentResolver;
            this.result = result;
        }

        @Override
        protected byte[] doInBackground(final Void... params) {
            // Load avatar for each contact identifier.
            return loadContactPhotoHighRes(contact.identifier, highRes, contentResolver);
        }

        @Override
        protected void onPostExecute(final byte[] avatar) {
            result.success(avatar);
        }

    }

    private static byte[] loadContactPhotoHighRes(final String lookUpKey,
                                                  final boolean photoHighResolution, final ContentResolver contentResolver) {
        try {
            final Uri uri = Uri.withAppendedPath(ContactsContract.Contacts.CONTENT_LOOKUP_URI, lookUpKey);
            final InputStream input = ContactsContract.Contacts.openContactPhotoInputStream(contentResolver, uri, photoHighResolution);

            if (input == null) return null;

            final Bitmap bitmap = BitmapFactory.decodeStream(input);
            input.close();

            final ByteArrayOutputStream stream = new ByteArrayOutputStream();
            bitmap.compress(Bitmap.CompressFormat.PNG, 100, stream);
            final byte[] bytes = stream.toByteArray();
            stream.close();
            return bytes;
        } catch (final IOException ex) {
            Log.e(LOG_TAG, ex.getMessage());
            return null;
        }
    }

    private boolean addContact(Contact contact) {
        try {
            ArrayList<ContentProviderOperation> ops = getAddContactOperations(contact);

            contentResolver.applyBatch(ContactsContract.AUTHORITY, ops);

            return true;
        } catch (Exception e) {
            Log.e("TAG", "Exception encountered while inserting contact: ");
            e.printStackTrace();
            return false;
        }
    }

    private String getLookupKeyFromContactId(String contactId) {
        final String[] projection = new String[]{ContactsContract.Contacts.LOOKUP_KEY};
        String selection = _ID + " = ?";
        String[] selectionArgs = new String[]{contactId};
        Cursor contactCursor = contentResolver.query(ContactsContract.Contacts.CONTENT_URI, projection, selection, selectionArgs, null);

        if (contactCursor != null && contactCursor.getCount() > 0) {
            contactCursor.moveToPosition(0);

            String lookupKey = contactCursor.getString(contactCursor.getColumnIndex(ContactsContract.Contacts.LOOKUP_KEY));
            contactCursor.close();
            return lookupKey;
        }
        return null;
    }

    private String getContactIdFromLookupKey(String lookupKey) {

        if (lookupKey == null || lookupKey.isEmpty()) {
            return null;
        }

        final String[] projection = new String[]{_ID};
        Uri lookupUri = Uri.withAppendedPath(ContactsContract.Contacts.CONTENT_LOOKUP_URI, lookupKey);
        Uri uri = ContactsContract.Contacts.getLookupUri(contentResolver, lookupUri);

        if (uri == null) {
            return null;
        }

        Cursor contactCursor = contentResolver.query(uri, projection, null, null, null);

        if (contactCursor != null && contactCursor.getCount() > 0) {
            contactCursor.moveToPosition(0);

            String contactId = contactCursor.getString(contactCursor.getColumnIndex(_ID));
            contactCursor.close();
            return contactId;
        }
        return null;
    }

    private String getNamedContactIdFromLookupKey(String lookupKey) {
        final String[] projection = new String[]{ContactsContract.Contacts.NAME_RAW_CONTACT_ID};
        Uri lookupUri = Uri.withAppendedPath(ContactsContract.Contacts.CONTENT_LOOKUP_URI, lookupKey);
        Uri uri = ContactsContract.Contacts.getLookupUri(contentResolver, lookupUri);

        if (uri == null) {
            return null;
        }

        Cursor contactCursor = contentResolver.query(uri, projection, null, null, null);

        if (contactCursor != null && contactCursor.getCount() > 0) {
            contactCursor.moveToPosition(0);

            String namedRawContactId = contactCursor.getString(contactCursor.getColumnIndex(ContactsContract.Contacts.NAME_RAW_CONTACT_ID));
            contactCursor.close();
            return namedRawContactId;
        }
        return null;
    }

    private String addContactWithReturnIdentifier(Contact contact) {
        try {
            ArrayList<ContentProviderOperation> ops = getAddContactOperations(contact);

            ContentProviderResult[] results = contentResolver.applyBatch(ContactsContract.AUTHORITY, ops);
            long contactId = 0;
            final String[] projection = new String[]{ContactsContract.RawContacts.CONTACT_ID};
            final Cursor cursor = contentResolver.query(results[0].uri, projection, null, null, null);
            if (cursor != null && cursor.getCount() > 0) {
                while (cursor.moveToNext()) {
                    contactId = cursor.getLong(0);
                }
                cursor.close();
            }

            if (contactId > 0) {
                String contactIdStr = String.valueOf(contactId);
                return getLookupKeyFromContactId(contactIdStr);
            }

        } catch (Exception e) {
            Log.e("TAG", "Exception encountered while inserting contact: ");
            e.printStackTrace();
        }
        return "";
    }

    private ArrayList<ContentProviderOperation> getAddContactOperations(Contact contact) {
        ArrayList<ContentProviderOperation> ops = new ArrayList<>();

        ContentProviderOperation.Builder op = ContentProviderOperation.newInsert(ContactsContract.RawContacts.CONTENT_URI)
                .withValue(ContactsContract.RawContacts.ACCOUNT_TYPE, contact.androidAccountType)
                .withValue(ContactsContract.RawContacts.ACCOUNT_NAME, contact.androidAccountName);
        ops.add(op.build());

        // Names
        op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE)
                .withValue(StructuredName.GIVEN_NAME, contact.givenName)
                .withValue(StructuredName.MIDDLE_NAME, contact.middleName)
                .withValue(StructuredName.FAMILY_NAME, contact.familyName)
                .withValue(StructuredName.PREFIX, contact.prefix)
                .withValue(StructuredName.SUFFIX, contact.suffix)
                .withValue(StructuredName.PHONETIC_GIVEN_NAME, contact.phoneticGivenName)
                .withValue(StructuredName.PHONETIC_MIDDLE_NAME, contact.phoneticMiddleName)
                .withValue(StructuredName.PHONETIC_FAMILY_NAME, contact.phoneticFamilyName);

        ops.add(op.build());

        // Note
        op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Note.CONTENT_ITEM_TYPE)
                .withValue(CommonDataKinds.Note.NOTE, contact.note);
        ops.add(op.build());

        // Sip
        op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.SipAddress.CONTENT_ITEM_TYPE)
                .withValue(CommonDataKinds.SipAddress.SIP_ADDRESS, contact.sip);
        ops.add(op.build());

        // Nickname
        op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Nickname.CONTENT_ITEM_TYPE)
                .withValue(CommonDataKinds.Nickname.NAME, contact.nickname);
        ops.add(op.build());

        // Organisation
        op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Organization.CONTENT_ITEM_TYPE)
                .withValue(Organization.COMPANY, contact.company)
                .withValue(Organization.TITLE, contact.jobTitle)
                .withValue(Organization.DEPARTMENT, contact.department);
        ops.add(op.build());

        //Photo
        op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                .withValue(ContactsContract.Data.IS_SUPER_PRIMARY, 1)
                .withValue(CommonDataKinds.Photo.PHOTO, contact.avatar)
                .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Photo.CONTENT_ITEM_TYPE);
        ops.add(op.build());

        op.withYieldAllowed(true);

        //Phones
        for (Item phone : contact.phones) {
            op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                    .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                    .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Phone.CONTENT_ITEM_TYPE)
                    .withValue(CommonDataKinds.Phone.NUMBER, phone.value)
                    .withValue(CommonDataKinds.Phone.LABEL, phone.label)
                    .withValue(CommonDataKinds.Phone.TYPE, Item.stringToPhoneType(phone.label));

            ops.add(op.build());
        }

        //Emails
        for (Item email : contact.emails) {
            op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                    .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                    .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Email.CONTENT_ITEM_TYPE)
                    .withValue(CommonDataKinds.Email.ADDRESS, email.value)
                    .withValue(CommonDataKinds.Email.LABEL, email.label)
                    .withValue(CommonDataKinds.Email.TYPE, Item.stringToEmailType(email.label));

            ops.add(op.build());
        }
        //Postal addresses
        for (PostalAddress address : contact.postalAddresses) {
            op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                    .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                    .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.StructuredPostal.CONTENT_ITEM_TYPE)
                    .withValue(CommonDataKinds.StructuredPostal.LABEL, address.label)
                    .withValue(CommonDataKinds.StructuredPostal.STREET, address.street)
                    .withValue(CommonDataKinds.StructuredPostal.NEIGHBORHOOD, address.locality)
                    .withValue(CommonDataKinds.StructuredPostal.CITY, address.city)
                    .withValue(CommonDataKinds.StructuredPostal.REGION, address.region)
                    .withValue(CommonDataKinds.StructuredPostal.POSTCODE, address.postcode)
                    .withValue(CommonDataKinds.StructuredPostal.COUNTRY, address.country)
                    .withValue(CommonDataKinds.StructuredPostal.TYPE, PostalAddress.stringToPostalAddressType(address.label));

            ops.add(op.build());
        }

        // Birthday
        op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Event.CONTENT_ITEM_TYPE)
                .withValue(CommonDataKinds.Event.TYPE, CommonDataKinds.Event.TYPE_BIRTHDAY)
                .withValue(CommonDataKinds.Event.START_DATE, contact.birthday);
        ops.add(op.build());

        // Other dates/events
        for (Item date : contact.dates) {
            op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                    .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                    .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Event.CONTENT_ITEM_TYPE)
                    .withValue(CommonDataKinds.Event.LABEL, date.label)
                    .withValue(CommonDataKinds.Event.TYPE, Item.stringToDatesType(date.label))
                    .withValue(CommonDataKinds.Event.START_DATE, date.value);

            ops.add(op.build());
        }

        // Website
        for (Item website : contact.websites) {
            op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                    .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                    .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Website.CONTENT_ITEM_TYPE)
                    .withValue(CommonDataKinds.Website.LABEL, website.label)
                    .withValue(CommonDataKinds.Website.TYPE, Item.stringToWebsiteType(website.label))
                    .withValue(CommonDataKinds.Website.URL, website.value);

            ops.add(op.build());
        }

        // Instant message address
        for (Item im : contact.instantMessageAddresses) {
            op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                    .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                    .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Im.CONTENT_ITEM_TYPE)
                    .withValue(CommonDataKinds.Im.PROTOCOL, Item.stringToInstantMessageAddressProtocol(im.label))
                    .withValue(CommonDataKinds.Im.DATA, im.value);
            ops.add(op.build());
        }
        // Relations
        for (Item relation : contact.relations) {
            op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                    .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                    .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Relation.CONTENT_ITEM_TYPE)
                    .withValue(CommonDataKinds.Relation.LABEL, relation.label)
                    .withValue(CommonDataKinds.Relation.TYPE, Item.stringToRelationType(relation.label))
                    .withValue(CommonDataKinds.Relation.NAME, relation.value);
            ops.add(op.build());
        }

        //Labels
        for (String label : contact.labels) {
            long groupId = getLabelGroupId(label);
            if (groupId < 0L) {
                groupId = insertLabelGroup(label);
            }
            if (groupId > 0L) {
                op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                        .withValueBackReference(ContactsContract.Data.RAW_CONTACT_ID, 0)
                        .withValue(CommonDataKinds.StructuredName.MIMETYPE, CommonDataKinds.GroupMembership.CONTENT_ITEM_TYPE)
                        .withValue(ContactsContract.CommonDataKinds.GroupMembership.GROUP_ROW_ID, groupId);
                ops.add(op.build());
            }
        }

        return ops;
    }

    private long insertLabelGroup(String groupTitle) {
        ContentValues contentValues = new ContentValues();
        contentValues.put(ContactsContract.Groups.TITLE, groupTitle);
        Uri groupUri = contentResolver.insert(ContactsContract.Groups.CONTENT_URI, contentValues);
        return ContentUris.parseId(groupUri);
    }

    private long getLabelGroupId(String groupTitle) {
        long groupId = -1;

        String projection[] = {ContactsContract.Groups._ID};
        String selection = ContactsContract.Groups.TITLE + " = ? ";
        String selectionArgs[] = new String[]{groupTitle};

        Cursor cursor = contentResolver.query(ContactsContract.Groups.CONTENT_URI, projection, selection, selectionArgs, null);
        if (cursor != null && cursor.getCount() > 0) {
            while (cursor.moveToNext()) {
                groupId = cursor.getLong(cursor.getColumnIndex(ContactsContract.Groups._ID));
            }
            cursor.close();
        }
        return groupId;
    }

    private ArrayList<String> getLabelGroupTitle(String groupId) {
        ArrayList<String> groupTitle = new ArrayList();

        String projection[] = {ContactsContract.Groups.TITLE};
        String selection = ContactsContract.Groups._ID + " = ? ";
        String selectionArgs[] = new String[]{groupId};

        Cursor cursor = contentResolver.query(ContactsContract.Groups.CONTENT_URI, projection, selection, selectionArgs, null);
        if (cursor != null && cursor.getCount() > 0) {
            while (cursor.moveToNext()) {
                groupTitle.add(cursor.getString(cursor.getColumnIndex(ContactsContract.Groups.TITLE)));
            }
            cursor.close();
        }
        return groupTitle;
    }

    public String getRawContactId(String lookupKey) {

        String contactId = getContactIdFromLookupKey(lookupKey);

        if (contactId == null) {
            return null;
        }

        String res = "";

        Uri uri = ContactsContract.RawContacts.CONTENT_URI;
        String[] projection = new String[]{ContactsContract.RawContacts._ID};
        String selection = ContactsContract.RawContacts.CONTACT_ID + " = ?";
        String[] selectionArgs = new String[]{contactId};

        Cursor contactIdCursor = contentResolver.query(uri, projection, selection, selectionArgs, null);
        if (contactIdCursor != null && contactIdCursor.moveToFirst()) {
            res = contactIdCursor.getString(contactIdCursor.getColumnIndex(ContactsContract.RawContacts._ID));
            contactIdCursor.close();
        }
        return res;
    }

    private boolean deleteContactsByIdentifiers(List<String> lookupKeyList) {
        ArrayList<ContentProviderOperation> ops = new ArrayList<>();

        if (lookupKeyList != null) {
            ArrayList<String> contactIdList = new ArrayList<>();
            for (String lookupKey : lookupKeyList) {
                String contactId = getContactIdFromLookupKey(lookupKey);
                if (contactId != null) {
                    contactIdList.add(contactId);
                }
            }

            for (String contactId : contactIdList) {
                Uri uri = Uri.withAppendedPath(ContactsContract.Contacts.CONTENT_URI, contactId);
                if (uri != null) {
                    ops.add(ContentProviderOperation.newDelete(uri).build());
                }
            }
            if (ops.size() > 0) {
                try {
                    contentResolver.applyBatch(ContactsContract.AUTHORITY, ops);
                    return true;
                } catch (Exception e) {
                    Log.e("TAG", "Exception encountered while deleting contacts by identifiers: ");
                    e.printStackTrace();
                    return false;
                }
            }
        }
        return true;
    }

    private boolean deleteContact(Contact contact) {
        if (contact == null || contact.identifier == null) {
            return false;
        }
        List<String> list = new ArrayList();
        list.add(contact.identifier);
        return deleteContactsByIdentifiers(list);
    }

    private boolean updateContact(Contact contact) {
        Log.e(this.getClass().getSimpleName(), "updateContact");
        try {
            ArrayList<ContentProviderOperation> ops = new ArrayList<>();
            ContentProviderOperation.Builder op = null;

            if (contact.identifier == null || contact.identifier.isEmpty()) {
                return false;
            }

            // Get contact id
            String rawContactId = getNamedContactIdFromLookupKey(contact.identifier);
            if (rawContactId == null) {
                Log.e(this.getClass().getSimpleName(), "Raw id is null for " + contact.identifier);

                return false;
            }

            // fetch current contact

            List<String> identifiers = new ArrayList<>();
            identifiers.add(contact.identifier);
            Cursor cursor = getCursorForContactIdentifiers(identifiers, true);

            String structureNameId = null;
            String organizationId = null;
            String nicknameId = null;
            String sipId = null;
            String noteId = null;
            Map<String, String> existingLabelIdMap = new HashMap<>();
            String birthdayId = null;
            while (cursor != null && cursor.moveToNext()) {
                String id = cursor.getString(cursor.getColumnIndex(BaseColumns._ID));
                String mimeType = cursor.getString(cursor.getColumnIndex(ContactsContract.Data.MIMETYPE));

                if (mimeType.equals(CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE)) {
                    structureNameId = id;
                } else if (mimeType.equals(CommonDataKinds.Organization.CONTENT_ITEM_TYPE)) {
                    organizationId = id;
                } else if (mimeType.equals(CommonDataKinds.Nickname.CONTENT_ITEM_TYPE)) {
                    nicknameId = id;
                } else if (mimeType.equals(CommonDataKinds.SipAddress.CONTENT_ITEM_TYPE)) {
                    sipId = id;
                } else if (mimeType.equals(CommonDataKinds.Note.CONTENT_ITEM_TYPE)) {
                    noteId = id;
                } else if (mimeType.equals(CommonDataKinds.GroupMembership.CONTENT_ITEM_TYPE)) {
                    String groupId = cursor.getString(cursor.getColumnIndex(CommonDataKinds.GroupMembership.DATA1));
                    existingLabelIdMap.put(groupId, id);
                } else if (mimeType.equals(CommonDataKinds.Event.CONTENT_ITEM_TYPE)) {
                    int eventType = cursor.getInt(cursor.getColumnIndex(CommonDataKinds.Event.TYPE));
                    if (eventType == CommonDataKinds.Event.TYPE_BIRTHDAY) {
                        birthdayId = id;
                    }
                }
            }

            ArrayList<Contact> contactList = getContactsFrom(cursor);

            if (contactList.size() == 0) {
                return false;
            }

            Contact currentContact = contactList.get(0);

            String queryCommon = BaseColumns._ID + "=? AND " + ContactsContract.Data.MIMETYPE + "=?";
            if (structureNameId == null) {
                Log.e(this.getClass().getSimpleName(), "Inserting structure name");
                // insert
                op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI);
                op.withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId);
                op.withValue(ContactsContract.Data.MIMETYPE, ContactsContract.CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE);
            } else {
                Log.e(this.getClass().getSimpleName(), "Updating structure id :" + structureNameId);
                // update
                if (equalsStructureName(contact, currentContact)) {
                    op = null;
                } else {
                    op = ContentProviderOperation.newUpdate(ContactsContract.Data.CONTENT_URI);
                    String[] queryArg = new String[]{structureNameId, ContactsContract.CommonDataKinds.StructuredName.CONTENT_ITEM_TYPE};
                    op.withSelection(queryCommon, queryArg);
                }
            }

            if (op != null) {
                // Update data (name)
                op.withValue(StructuredName.GIVEN_NAME, contact.givenName)
                        .withValue(StructuredName.MIDDLE_NAME, contact.middleName)
                        .withValue(StructuredName.FAMILY_NAME, contact.familyName)
                        .withValue(StructuredName.PREFIX, contact.prefix)
                        .withValue(StructuredName.SUFFIX, contact.suffix)
                        .withValue(StructuredName.PHONETIC_GIVEN_NAME, contact.phoneticGivenName)
                        .withValue(StructuredName.PHONETIC_MIDDLE_NAME, contact.phoneticMiddleName)
                        .withValue(StructuredName.PHONETIC_FAMILY_NAME, contact.phoneticFamilyName);
                ops.add(op.build());
            }

            if (organizationId == null) {
                Log.e(this.getClass().getSimpleName(), "Inserting organization name");
                // insert
                op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                        .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Organization.CONTENT_ITEM_TYPE)
                        .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId);
            } else {
                Log.e(this.getClass().getSimpleName(), "Updating organization id :" + organizationId);
                // update
                if (equalsOrganization(contact, currentContact)) {
                    op = null;
                } else {
                    op = ContentProviderOperation.newUpdate(ContactsContract.Data.CONTENT_URI);
                    String[] queryArg = new String[]{organizationId, ContactsContract.CommonDataKinds.Organization.CONTENT_ITEM_TYPE};
                    op.withSelection(queryCommon, queryArg);
                }
            }

            if (op != null) {
                op.withValue(Organization.TYPE, Organization.TYPE_WORK)
                        .withValue(Organization.COMPANY, contact.company)
                        .withValue(Organization.DEPARTMENT, contact.department)
                        .withValue(Organization.TITLE, contact.jobTitle);
                ops.add(op.build());
            }

            if (nicknameId == null) {
                Log.e(this.getClass().getSimpleName(), "Inserting nickname");
                // insert
                op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                        .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Nickname.CONTENT_ITEM_TYPE)
                        .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId);
            } else {
                Log.e(this.getClass().getSimpleName(), "Updating nickname id :" + nicknameId);
                // update
                if (equalsStrings(contact.nickname, currentContact.nickname)) {
                    op = null;
                } else {
                    op = ContentProviderOperation.newUpdate(ContactsContract.Data.CONTENT_URI);
                    String[] queryArg = new String[]{nicknameId, ContactsContract.CommonDataKinds.Nickname.CONTENT_ITEM_TYPE};
                    op.withSelection(queryCommon, queryArg);
                }
            }

            if (op != null) {
                op.withValue(CommonDataKinds.Nickname.NAME, contact.nickname);
                ops.add(op.build());
            }

            if (sipId == null) {
                Log.e(this.getClass().getSimpleName(), "Inserting sip id");
                // insert
                op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                        .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.SipAddress.CONTENT_ITEM_TYPE)
                        .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId);
            } else {
                Log.e(this.getClass().getSimpleName(), "Updating sip id :" + sipId);
                // update
                if (equalsStrings(contact.sip, currentContact.sip)) {
                    op = null;
                } else {
                    op = ContentProviderOperation.newUpdate(ContactsContract.Data.CONTENT_URI);
                    String[] queryArg = new String[]{sipId, ContactsContract.CommonDataKinds.SipAddress.CONTENT_ITEM_TYPE};
                    op.withSelection(queryCommon, queryArg);
                }
            }
            if (op != null) {
                op.withValue(CommonDataKinds.SipAddress.SIP_ADDRESS, contact.sip);
                ops.add(op.build());
            }

            if (noteId == null) {
                Log.e(this.getClass().getSimpleName(), "Inserting note id");
                // insert
                op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                        .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Note.CONTENT_ITEM_TYPE)
                        .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId);
            } else {
                Log.e(this.getClass().getSimpleName(), "Updating note id :" + noteId);
                // update
                if (equalsStrings(contact.note, currentContact.note)) {
                    op = null;
                } else {
                    op = ContentProviderOperation.newUpdate(ContactsContract.Data.CONTENT_URI);
                    String[] queryArg = new String[]{noteId, ContactsContract.CommonDataKinds.Note.CONTENT_ITEM_TYPE};
                    op.withSelection(queryCommon, queryArg);
                }
            }

            if (op != null) {
                op.withValue(CommonDataKinds.Note.NOTE, contact.note);
                ops.add(op.build());
            }

            addEmailsUpdateOperations(rawContactId, currentContact.emails, contact.emails, ops);
            addPhoneUpdateOperations(rawContactId, currentContact.phones, contact.phones, ops);
            addPostalAddressUpdateOperations(rawContactId, currentContact.postalAddresses, contact.postalAddresses, ops);
            addWebsiteUpdateOperations(rawContactId, currentContact.websites, contact.websites, ops);
            addImUpdateOperations(rawContactId, currentContact.instantMessageAddresses, contact.instantMessageAddresses, ops);
            addRelationUpdateOperations(rawContactId, currentContact.relations, contact.relations, ops);
            addLabelUpdateOperations(rawContactId, existingLabelIdMap, currentContact.labels, contact.labels, ops);
            addEventUpdateOperations(rawContactId, currentContact.dates, contact.dates, ops);
            addBirthdayOperation(rawContactId, birthdayId, currentContact.birthday, contact.birthday, ops);

            ContentProviderResult[] results = contentResolver.applyBatch(ContactsContract.AUTHORITY, ops);
            return true;
        } catch (Exception e) {
            // Log exception
            Log.e(this.getClass().getSimpleName(), "Exception encountered while updating contact: ");
            e.printStackTrace();
            return false;
        }
    }

    private boolean equalsStructureName(Contact contact, Contact currentContact) {

        if (contact == null && currentContact == null) {
            return true;
        }
        if (contact == null || currentContact == null) {
            return false;
        }
        if (!equalsStrings(contact.givenName, currentContact.givenName)) {
            return false;
        }
        if (!equalsStrings(contact.middleName, currentContact.middleName)) {
            return false;
        }
        if (!equalsStrings(contact.familyName, currentContact.familyName)) {
            return false;
        }
        if (!equalsStrings(contact.prefix, currentContact.prefix)) {
            return false;
        }
        if (!equalsStrings(contact.suffix, currentContact.suffix)) {
            return false;
        }
        if (!equalsStrings(contact.phoneticGivenName, currentContact.phoneticGivenName)) {
            return false;
        }
        if (!equalsStrings(contact.phoneticMiddleName, currentContact.phoneticMiddleName)) {
            return false;
        }
        if (!equalsStrings(contact.phoneticFamilyName, currentContact.phoneticFamilyName)) {
            return false;
        }

        return true;
    }

    private boolean equalsOrganization(Contact contact, Contact currentContact) {

        if (contact == null && currentContact == null) {
            return true;
        }
        if (contact == null || currentContact == null) {
            return false;
        }
        if (!equalsStrings(contact.company, currentContact.company)) {
            return false;
        }
        if (!equalsStrings(contact.department, currentContact.department)) {
            return false;
        }
        if (!equalsStrings(contact.jobTitle, currentContact.jobTitle)) {
            return false;
        }
        return true;
    }

    private void addEmailsUpdateOperations(String rawContactId, ArrayList<Item> existingItemList, ArrayList<Item> newItemList,
                                           ArrayList<ContentProviderOperation> ops) {
        ContentProviderOperation.Builder op;
        String queryCommon = BaseColumns._ID + "=? AND " + ContactsContract.Data.MIMETYPE + "=?";
        List<String> existingIdList = new ArrayList<>();
        for (Item item : existingItemList) {
            existingIdList.add(item.identifier);
        }
        if (existingIdList.size() > 0 && newItemList.size() == 0) {
            // remove all current ids
            for (String id : existingIdList) {
                String[] queryArg = new String[]{id, ContactsContract.CommonDataKinds.Email.CONTENT_ITEM_TYPE};
                op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                        .withSelection(queryCommon, queryArg);
                ops.add(op.build());
            }
        } else if (newItemList.size() > 0 && existingIdList.size() == 0) {
            // insert all incoming ids
            for (Item item : newItemList) {
                op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                        .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Email.CONTENT_ITEM_TYPE)
                        .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                        .withValue(CommonDataKinds.Email.ADDRESS, item.value)
                        .withValue(Email.LABEL, item.label)
                        .withValue(CommonDataKinds.Email.TYPE, Item.stringToEmailType(item.label));
                ops.add(op.build());
            }
        } else {
            for (Item item : newItemList) {
                if (item.identifier == null || item.identifier.isEmpty()) {
                    // insert
                    op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                            .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Email.CONTENT_ITEM_TYPE)
                            .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                            .withValue(CommonDataKinds.Email.ADDRESS, item.value)
                            .withValue(Email.LABEL, item.label)
                            .withValue(CommonDataKinds.Email.TYPE, Item.stringToEmailType(item.label));
                    ops.add(op.build());
                } else if (existingIdList.contains(item.identifier)) {
                    // found update this item
                    for (Item existing : existingItemList) {
                        if (item.identifier.equals(existing.identifier)) {
                            if (!item.equalValues(existing)) {
                                String[] queryArg = new String[]{item.identifier, ContactsContract.CommonDataKinds.Email.CONTENT_ITEM_TYPE};
                                op = ContentProviderOperation.newUpdate(ContactsContract.Data.CONTENT_URI)
                                        .withSelection(queryCommon, queryArg)
                                        .withValue(CommonDataKinds.Email.ADDRESS, item.value)
                                        .withValue(Email.LABEL, item.label)
                                        .withValue(CommonDataKinds.Email.TYPE, Item.stringToEmailType(item.label));
                                ops.add(op.build());
                            }
                            existingIdList.remove(item.identifier);
                        }
                    }

                }
            }

            if (existingIdList.size() > 0) {
                // remove all ids
                for (String id : existingIdList) {
                    String[] queryArg = new String[]{id, ContactsContract.CommonDataKinds.Email.CONTENT_ITEM_TYPE};
                    op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                            .withSelection(queryCommon, queryArg);
                    ops.add(op.build());
                }
            }
        }
    }

    private void addPhoneUpdateOperations(String rawContactId, List<Item> existingItemList, ArrayList<Item> newItemList,
                                          ArrayList<ContentProviderOperation> ops) {
        ContentProviderOperation.Builder op;
        String queryCommon = BaseColumns._ID + "=? AND " + ContactsContract.Data.MIMETYPE + "=?";
        List<String> existingIdList = new ArrayList<>();
        for (Item item : existingItemList) {
            existingIdList.add(item.identifier);
        }
        if (existingIdList.size() > 0 && newItemList.size() == 0) {
            // remove all current ids
            for (String id : existingIdList) {
                String[] queryArg = new String[]{id, ContactsContract.CommonDataKinds.Phone.CONTENT_ITEM_TYPE};
                op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                        .withSelection(queryCommon, queryArg);
                ops.add(op.build());
            }
        } else if (newItemList.size() > 0 && existingIdList.size() == 0) {
            // insert all incoming ids
            for (Item item : newItemList) {
                op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                        .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Phone.CONTENT_ITEM_TYPE)
                        .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                        .withValue(CommonDataKinds.Phone.NUMBER, item.value)
                        .withValue(CommonDataKinds.Phone.LABEL, item.label)
                        .withValue(CommonDataKinds.Phone.TYPE, Item.stringToPhoneType(item.label));
                ops.add(op.build());
            }
        } else {
            for (Item item : newItemList) {
                if (item.identifier == null || item.identifier.isEmpty()) {
                    // insert
                    op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                            .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Phone.CONTENT_ITEM_TYPE)
                            .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                            .withValue(CommonDataKinds.Phone.NUMBER, item.value)
                            .withValue(CommonDataKinds.Phone.LABEL, item.label)
                            .withValue(CommonDataKinds.Phone.TYPE, Item.stringToPhoneType(item.label));
                    ops.add(op.build());
                } else if (existingIdList.contains(item.identifier)) {
                    // found update this item
                    for (Item existing : existingItemList) {
                        if (item.identifier.equals(existing.identifier)) {
                            if (!item.equalValues(existing)) {
                                String[] queryArg = new String[]{item.identifier, ContactsContract.CommonDataKinds.Phone.CONTENT_ITEM_TYPE};
                                op = ContentProviderOperation.newUpdate(ContactsContract.Data.CONTENT_URI)
                                        .withSelection(queryCommon, queryArg)
                                        .withValue(CommonDataKinds.Phone.NUMBER, item.value)
                                        .withValue(CommonDataKinds.Phone.LABEL, item.label)
                                        .withValue(CommonDataKinds.Phone.TYPE, Item.stringToPhoneType(item.label));
                                ops.add(op.build());
                            }
                            existingIdList.remove(item.identifier);
                        }
                    }
                }
            }

            if (existingIdList.size() > 0) {
                // remove all ids
                for (String id : existingIdList) {
                    String[] queryArg = new String[]{id, ContactsContract.CommonDataKinds.Phone.CONTENT_ITEM_TYPE};
                    op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                            .withSelection(queryCommon, queryArg);
                    ops.add(op.build());
                }
            }
        }
    }

    private void addPostalAddressUpdateOperations(String rawContactId, List<PostalAddress> existingItemList, ArrayList<PostalAddress> newItemList,
                                                  ArrayList<ContentProviderOperation> ops) {
        ContentProviderOperation.Builder op;
        String queryCommon = BaseColumns._ID + "=? AND " + ContactsContract.Data.MIMETYPE + "=?";
        List<String> existingIdList = new ArrayList<>();
        for (PostalAddress item : existingItemList) {
            existingIdList.add(item.identifier);
        }
        if (existingIdList.size() > 0 && newItemList.size() == 0) {
            // remove all current ids
            for (String id : existingIdList) {
                String[] queryArg = new String[]{id, ContactsContract.CommonDataKinds.StructuredPostal.CONTENT_ITEM_TYPE};
                op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                        .withSelection(queryCommon, queryArg);
                ops.add(op.build());
            }
        } else if (newItemList.size() > 0 && existingIdList.size() == 0) {
            // insert all incoming ids
            for (PostalAddress item : newItemList) {
                op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                        .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.StructuredPostal.CONTENT_ITEM_TYPE)
                        .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                        .withValue(StructuredPostal.LABEL, item.label)
                        .withValue(StructuredPostal.TYPE, PostalAddress.stringToPostalAddressType(item.label))
                        .withValue(StructuredPostal.STREET, item.street)
                        .withValue(StructuredPostal.NEIGHBORHOOD, item.locality)
                        .withValue(StructuredPostal.CITY, item.city)
                        .withValue(StructuredPostal.REGION, item.region)
                        .withValue(StructuredPostal.POSTCODE, item.postcode)
                        .withValue(StructuredPostal.COUNTRY, item.country);
                ops.add(op.build());
            }
        } else {
            for (PostalAddress item : newItemList) {
                if (item.identifier == null || item.identifier.isEmpty()) {
                    // insert
                    op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                            .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.StructuredPostal.CONTENT_ITEM_TYPE)
                            .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                            .withValue(StructuredPostal.LABEL, item.label)
                            .withValue(StructuredPostal.TYPE, PostalAddress.stringToPostalAddressType(item.label))
                            .withValue(StructuredPostal.STREET, item.street)
                            .withValue(StructuredPostal.NEIGHBORHOOD, item.locality)
                            .withValue(StructuredPostal.CITY, item.city)
                            .withValue(StructuredPostal.REGION, item.region)
                            .withValue(StructuredPostal.POSTCODE, item.postcode)
                            .withValue(StructuredPostal.COUNTRY, item.country);
                    ops.add(op.build());
                } else if (existingIdList.contains(item.identifier)) {
                    // found update this item
                    for (PostalAddress existing : existingItemList) {
                        if (item.identifier.equals(existing.identifier)) {
                            if (!item.equalValues(existing)) {
                                String[] queryArg = new String[]{item.identifier, ContactsContract.CommonDataKinds.StructuredPostal.CONTENT_ITEM_TYPE};
                                op = ContentProviderOperation.newUpdate(ContactsContract.Data.CONTENT_URI)
                                        .withSelection(queryCommon, queryArg)
                                        .withValue(StructuredPostal.LABEL, item.label)
                                        .withValue(StructuredPostal.TYPE, PostalAddress.stringToPostalAddressType(item.label))
                                        .withValue(StructuredPostal.STREET, item.street)
                                        .withValue(StructuredPostal.NEIGHBORHOOD, item.locality)
                                        .withValue(StructuredPostal.CITY, item.city)
                                        .withValue(StructuredPostal.REGION, item.region)
                                        .withValue(StructuredPostal.POSTCODE, item.postcode)
                                        .withValue(StructuredPostal.COUNTRY, item.country);
                                ops.add(op.build());
                            }
                            existingIdList.remove(item.identifier);
                        }
                    }
                }
            }

            if (existingIdList.size() > 0) {
                // remove all ids
                for (String id : existingIdList) {
                    String[] queryArg = new String[]{id, ContactsContract.CommonDataKinds.StructuredPostal.CONTENT_ITEM_TYPE};
                    op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                            .withSelection(queryCommon, queryArg);
                    ops.add(op.build());
                }
            }
        }
    }

    private void addWebsiteUpdateOperations(String rawContactId, List<Item> existingItemList, ArrayList<Item> newItemList,
                                            ArrayList<ContentProviderOperation> ops) {

        Log.e(this.getClass().getSimpleName(), "addWebsiteUpdateOperations");

        ContentProviderOperation.Builder op;
        String queryCommon = BaseColumns._ID + "=? AND " + ContactsContract.Data.MIMETYPE + "=?";
        List<String> existingIdList = new ArrayList<>();
        for (Item item : existingItemList) {
            existingIdList.add(item.identifier);
        }
        if (existingIdList.size() > 0 && newItemList.size() == 0) {
            // remove all current ids
            for (String id : existingIdList) {

                Log.e(this.getClass().getSimpleName(), "addWebsiteUpdateOperations : remove : " + id);

                String[] queryArg = new String[]{id, ContactsContract.CommonDataKinds.Website.CONTENT_ITEM_TYPE};
                op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                        .withSelection(queryCommon, queryArg);
                ops.add(op.build());
            }
        } else if (newItemList.size() > 0 && existingIdList.size() == 0) {
            // insert all incoming ids
            for (Item item : newItemList) {

                Log.e(this.getClass().getSimpleName(), "addWebsiteUpdateOperations : insert");

                op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                        .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Website.CONTENT_ITEM_TYPE)
                        .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                        .withValue(CommonDataKinds.Website.LABEL, item.label)
                        .withValue(CommonDataKinds.Website.URL, item.value)
                        .withValue(CommonDataKinds.Website.TYPE, Item.stringToWebsiteType(item.label));
                ops.add(op.build());
            }
        } else {
            for (Item item : newItemList) {
                if (item.identifier == null || item.identifier.isEmpty()) {

                    Log.e(this.getClass().getSimpleName(), "addWebsiteUpdateOperations : insert");

                    // insert
                    op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                            .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Website.CONTENT_ITEM_TYPE)
                            .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                            .withValue(CommonDataKinds.Website.LABEL, item.label)
                            .withValue(CommonDataKinds.Website.URL, item.value)
                            .withValue(CommonDataKinds.Website.TYPE, Item.stringToWebsiteType(item.label));
                    ops.add(op.build());
                } else if (existingIdList.contains(item.identifier)) {
                    // found update this item
                    for (Item existing : existingItemList) {
                        if (item.identifier.equals(existing.identifier)) {
                            if (!item.equalValues(existing)) {
                                Log.e(this.getClass().getSimpleName(), "addWebsiteUpdateOperations : update " + item.identifier);
                                Log.e(this.getClass().getSimpleName(), "addWebsiteUpdateOperations : item " + item.toString());
                                Log.e(this.getClass().getSimpleName(), "addWebsiteUpdateOperations : existing " + existing.toString());

                                String[] queryArg = new String[]{item.identifier, ContactsContract.CommonDataKinds.Website.CONTENT_ITEM_TYPE};
                                op = ContentProviderOperation.newUpdate(ContactsContract.Data.CONTENT_URI)
                                        .withSelection(queryCommon, queryArg)
                                        .withValue(CommonDataKinds.Website.LABEL, item.label)
                                        .withValue(CommonDataKinds.Website.URL, item.value)
                                        .withValue(CommonDataKinds.Website.TYPE, Item.stringToWebsiteType(item.label));
                                ops.add(op.build());
                            }
                            existingIdList.remove(item.identifier);
                        }
                    }
                }
            }

            if (existingIdList.size() > 0) {
                // remove all ids
                for (String id : existingIdList) {

                    Log.e(this.getClass().getSimpleName(), "addWebsiteUpdateOperations : remove : " + id);

                    String[] queryArg = new String[]{id, ContactsContract.CommonDataKinds.Website.CONTENT_ITEM_TYPE};
                    op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                            .withSelection(queryCommon, queryArg);
                    ops.add(op.build());
                }
            }
        }
    }

    private void addImUpdateOperations(String rawContactId, List<Item> existingItemList, ArrayList<Item> newItemList,
                                       ArrayList<ContentProviderOperation> ops) {
        ContentProviderOperation.Builder op;
        String queryCommon = BaseColumns._ID + "=? AND " + ContactsContract.Data.MIMETYPE + "=?";
        List<String> existingIdList = new ArrayList<>();
        for (Item item : existingItemList) {
            existingIdList.add(item.identifier);
        }

        if (existingIdList.size() > 0 && newItemList.size() == 0) {
            // remove all current ids
            for (String id : existingIdList) {
                String[] queryArg = new String[]{id, ContactsContract.CommonDataKinds.Im.CONTENT_ITEM_TYPE};
                op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                        .withSelection(queryCommon, queryArg);
                ops.add(op.build());
            }
        } else if (newItemList.size() > 0 && existingIdList.size() == 0) {
            // insert all incoming ids
            for (Item item : newItemList) {
                op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                        .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Im.CONTENT_ITEM_TYPE)
                        .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                        .withValue(CommonDataKinds.Im.DATA, item.value)
                        .withValue(CommonDataKinds.Im.TYPE, Item.stringToInstantMessageAddressProtocol(item.label))
                        .withValue(CommonDataKinds.Im.PROTOCOL, Item.stringToInstantMessageAddressProtocol(item.label));

                if (Item.stringToInstantMessageAddressProtocol(item.label) == CommonDataKinds.Im.PROTOCOL_CUSTOM) {
                    op.withValue(CommonDataKinds.Im.CUSTOM_PROTOCOL, item.label);
                }

                ops.add(op.build());
            }
        } else {
            for (Item item : newItemList) {
                if (item.identifier == null || item.identifier.isEmpty()) {
                    // insert
                    op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                            .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Im.CONTENT_ITEM_TYPE)
                            .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                            .withValue(CommonDataKinds.Im.DATA, item.value)
                            .withValue(CommonDataKinds.Im.TYPE, Item.stringToInstantMessageAddressProtocol(item.label))
                            .withValue(CommonDataKinds.Im.PROTOCOL, Item.stringToInstantMessageAddressProtocol(item.label));

                    if (Item.stringToInstantMessageAddressProtocol(item.label) == CommonDataKinds.Im.PROTOCOL_CUSTOM) {
                        op.withValue(CommonDataKinds.Im.CUSTOM_PROTOCOL, item.label);
                    }

                    ops.add(op.build());
                } else if (existingIdList.contains(item.identifier)) {
                    // found update this item
                    for (Item existing : existingItemList) {
                        if (item.identifier.equals(existing.identifier)) {
                            if (!item.equalValues(existing)) {
                                String[] queryArg = new String[]{item.identifier, ContactsContract.CommonDataKinds.Im.CONTENT_ITEM_TYPE};

                                op = ContentProviderOperation.newUpdate(ContactsContract.Data.CONTENT_URI)
                                        .withSelection(queryCommon, queryArg)
                                        .withValue(CommonDataKinds.Im.DATA, item.value)
                                        .withValue(CommonDataKinds.Im.TYPE, Item.stringToInstantMessageAddressProtocol(item.label))
                                        .withValue(CommonDataKinds.Im.PROTOCOL, Item.stringToInstantMessageAddressProtocol(item.label));

                                if (Item.stringToInstantMessageAddressProtocol(item.label) == CommonDataKinds.Im.PROTOCOL_CUSTOM) {
                                    op.withValue(CommonDataKinds.Im.CUSTOM_PROTOCOL, item.label);
                                }

                                ops.add(op.build());
                            }
                            existingIdList.remove(item.identifier);
                        }
                    }
                }
            }

            if (existingIdList.size() > 0) {
                // remove all ids
                for (String id : existingIdList) {
                    String[] queryArg = new String[]{id, ContactsContract.CommonDataKinds.Im.CONTENT_ITEM_TYPE};
                    op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                            .withSelection(queryCommon, queryArg);
                    ops.add(op.build());
                }
            }
        }
    }

    private void addRelationUpdateOperations(String rawContactId, List<Item> existingItemList, ArrayList<Item> newItemList,
                                             ArrayList<ContentProviderOperation> ops) {
        ContentProviderOperation.Builder op;
        String queryCommon = BaseColumns._ID + "=? AND " + ContactsContract.Data.MIMETYPE + "=?";
        List<String> existingIdList = new ArrayList<>();
        for (Item item : existingItemList) {
            existingIdList.add(item.identifier);
        }
        if (existingIdList.size() > 0 && newItemList.size() == 0) {
            // remove all current ids
            for (String id : existingIdList) {
                String[] queryArg = new String[]{id, ContactsContract.CommonDataKinds.Relation.CONTENT_ITEM_TYPE};
                op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                        .withSelection(queryCommon, queryArg);
                ops.add(op.build());
            }
        } else if (newItemList.size() > 0 && existingIdList.size() == 0) {
            // insert all incoming ids
            for (Item item : newItemList) {
                op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                        .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Relation.CONTENT_ITEM_TYPE)
                        .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                        .withValue(CommonDataKinds.Relation.NAME, item.value)
                        .withValue(CommonDataKinds.Relation.LABEL, item.label)
                        .withValue(CommonDataKinds.Relation.TYPE, Item.stringToRelationType(item.label));

                ops.add(op.build());
            }
        } else {
            for (Item item : newItemList) {
                if (item.identifier == null || item.identifier.isEmpty()) {
                    // insert
                    op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                            .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Relation.CONTENT_ITEM_TYPE)
                            .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                            .withValue(CommonDataKinds.Relation.NAME, item.value)
                            .withValue(CommonDataKinds.Relation.LABEL, item.label)
                            .withValue(CommonDataKinds.Relation.TYPE, Item.stringToRelationType(item.label));

                    ops.add(op.build());
                } else if (existingIdList.contains(item.identifier)) {
                    // found update this item
                    for (Item existing : existingItemList) {
                        if (item.identifier.equals(existing.identifier)) {
                            if (!item.equalValues(existing)) {
                                String[] queryArg = new String[]{item.identifier, ContactsContract.CommonDataKinds.Relation.CONTENT_ITEM_TYPE};

                                op = ContentProviderOperation.newUpdate(ContactsContract.Data.CONTENT_URI)
                                        .withSelection(queryCommon, queryArg)
                                        .withValue(CommonDataKinds.Relation.NAME, item.value)
                                        .withValue(CommonDataKinds.Relation.LABEL, item.label)
                                        .withValue(CommonDataKinds.Relation.TYPE, Item.stringToRelationType(item.label));

                                ops.add(op.build());
                            }
                            existingIdList.remove(item.identifier);
                        }
                    }
                }
            }

            if (existingIdList.size() > 0) {
                // remove all ids
                for (String id : existingIdList) {
                    String[] queryArg = new String[]{id, ContactsContract.CommonDataKinds.Relation.CONTENT_ITEM_TYPE};
                    op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                            .withSelection(queryCommon, queryArg);
                    ops.add(op.build());
                }
            }
        }
    }

    private void addLabelUpdateOperations(String rawContactId, Map<String, String> existingIdMap, List<String> existingItemList,
                                          ArrayList<String> newItemList,
                                          ArrayList<ContentProviderOperation> ops) {

        Log.e(this.getClass().getSimpleName(), "addLabelUpdateOperations");

        ContentProviderOperation.Builder op;
        String queryCommon = BaseColumns._ID + "=? AND " + ContactsContract.Data.MIMETYPE + "=?";
        if (existingItemList.size() > 0 && newItemList.size() == 0) {
            // remove all current ids
            for (String item : existingItemList) {
                long groupId = getGroupId(item);
                if (groupId > 0L) {
                    String rowId = existingIdMap.get(groupId);
                    if (rowId != null) {

                        Log.e(this.getClass().getSimpleName(), "addLabelUpdateOperations : remove " + rowId);

                        String[] queryArg = new String[]{rowId, ContactsContract.CommonDataKinds.GroupMembership.CONTENT_ITEM_TYPE};
                        op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                                .withSelection(queryCommon, queryArg);
                        ops.add(op.build());
                    }
                }
            }
        } else if (newItemList.size() > 0 && existingItemList.size() == 0) {
            // insert all incoming ids
            for (String item : newItemList) {
                long groupId = getGroupId(item);
                if (groupId > 0L) {

                    Log.e(this.getClass().getSimpleName(), "addLabelUpdateOperations : insert " + groupId);

                    op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                            .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.GroupMembership.CONTENT_ITEM_TYPE)
                            .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                            .withValue(ContactsContract.CommonDataKinds.GroupMembership.GROUP_ROW_ID, groupId);
                    ops.add(op.build());
                }
            }
        } else {
            for (String item : existingItemList) {
                if (!newItemList.contains(item)) {
                    long groupId = getGroupId(item);
                    if (groupId > 0L) {
                        String rowId = existingIdMap.get(groupId);
                        if (rowId != null) {

                            Log.e(this.getClass().getSimpleName(), "addLabelUpdateOperations : remove " + rowId);

                            String[] queryArg = new String[]{rowId, ContactsContract.CommonDataKinds.GroupMembership.CONTENT_ITEM_TYPE};
                            op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                                    .withSelection(queryCommon, queryArg);
                            ops.add(op.build());
                        }
                    }
                }
            }

            for (String item : newItemList) {
                if (!existingItemList.contains(item)) {
                    long groupId = getGroupId(item);
                    if (groupId > 0L) {

                        Log.e(this.getClass().getSimpleName(), "addLabelUpdateOperations : insert " + groupId);

                        op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                                .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.GroupMembership.CONTENT_ITEM_TYPE)
                                .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                                .withValue(ContactsContract.CommonDataKinds.GroupMembership.GROUP_ROW_ID, groupId);
                        ops.add(op.build());
                    }
                }
            }
        }
    }

    private long getGroupId(String groupName) {
        long groupId = getLabelGroupId(groupName);
        if (groupId < 0L) {
            groupId = insertLabelGroup(groupName);
        }
        return groupId;
    }

    private void addBirthdayOperation(String rawContactId, String birthdayId, String existingBirthday, String newBirthday, ArrayList<ContentProviderOperation> ops) {

        Log.e(this.getClass().getSimpleName(), "addBirthdayOperation");
        Log.e(this.getClass().getSimpleName(), "existingBirthday : " + existingBirthday);
        Log.e(this.getClass().getSimpleName(), "newBirthday : " + newBirthday);

        ContentProviderOperation.Builder op;
        String queryCommon = BaseColumns._ID + "=? AND " + ContactsContract.Data.MIMETYPE + "=?";

        //process birthday
        if (newBirthday == null && existingBirthday != null) {
            Log.e(this.getClass().getSimpleName(), "delete birthday");
            //Delete birthday

            String[] queryArg = new String[]{birthdayId, ContactsContract.CommonDataKinds.Event.CONTENT_ITEM_TYPE};
            op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                    .withSelection(queryCommon, queryArg);
            ops.add(op.build());

        }
        if (newBirthday != null && existingBirthday == null) {
            Log.e(this.getClass().getSimpleName(), "insert birthday");
            //Insert birthday
            op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                    .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Event.CONTENT_ITEM_TYPE)
                    .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                    .withValue(CommonDataKinds.Event.START_DATE, newBirthday)
                    .withValue(CommonDataKinds.Event.TYPE, CommonDataKinds.Event.TYPE_BIRTHDAY);

            ops.add(op.build());
        } else if (newBirthday != null && existingBirthday != null && !existingBirthday.equals(newBirthday)) {
            Log.e(this.getClass().getSimpleName(), "update birthday");
            //Update birthday if values are not same
            String[] queryArg = new String[]{birthdayId, ContactsContract.CommonDataKinds.Event.CONTENT_ITEM_TYPE};

            op = ContentProviderOperation.newUpdate(ContactsContract.Data.CONTENT_URI)
                    .withSelection(queryCommon, queryArg)
                    .withValue(CommonDataKinds.Event.TYPE, CommonDataKinds.Event.TYPE_BIRTHDAY)
                    .withValue(CommonDataKinds.Event.START_DATE, newBirthday);
            ops.add(op.build());
        }
    }

    private void addEventUpdateOperations(String rawContactId, List<Item> existingItemList, List<Item> newItemList,
                                          ArrayList<ContentProviderOperation> ops) {

        Log.e(this.getClass().getSimpleName(), "addEventUpdateOperations");

        ContentProviderOperation.Builder op;
        String queryCommon = BaseColumns._ID + "=? AND " + ContactsContract.Data.MIMETYPE + "=?";

        List<String> existingIdList = new ArrayList<>();
        for (Item item : existingItemList) {
            existingIdList.add(item.identifier);
        }
        if (existingIdList.size() > 0 && newItemList.size() == 0) {
            // remove all current ids
            for (String id : existingIdList) {
                Log.e(this.getClass().getSimpleName(), "addEventUpdateOperations : remove " + id);
                String[] queryArg = new String[]{id, ContactsContract.CommonDataKinds.Event.CONTENT_ITEM_TYPE};
                op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                        .withSelection(queryCommon, queryArg);
                ops.add(op.build());
            }
        } else if (newItemList.size() > 0 && existingIdList.size() == 0) {
            // insert all incoming ids
            for (Item item : newItemList) {
                Log.e(this.getClass().getSimpleName(), "addEventUpdateOperations : insert 1 " + item.toString());
                op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                        .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Event.CONTENT_ITEM_TYPE)
                        .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                        .withValue(CommonDataKinds.Event.LABEL, item.label)
                        .withValue(CommonDataKinds.Event.START_DATE, item.value)
                        .withValue(CommonDataKinds.Event.TYPE, Item.stringToDatesType(item.label));

                ops.add(op.build());
            }
        } else {
            for (Item item : newItemList) {
                if (item.identifier == null || item.identifier.isEmpty()) {
                    // insert
                    Log.e(this.getClass().getSimpleName(), "addEventUpdateOperations : insert 2 " + item.toString());
                    op = ContentProviderOperation.newInsert(ContactsContract.Data.CONTENT_URI)
                            .withValue(ContactsContract.Data.MIMETYPE, CommonDataKinds.Event.CONTENT_ITEM_TYPE)
                            .withValue(ContactsContract.Data.RAW_CONTACT_ID, rawContactId)
                            .withValue(CommonDataKinds.Event.LABEL, item.label)
                            .withValue(CommonDataKinds.Event.START_DATE, item.value)
                            .withValue(CommonDataKinds.Event.TYPE, Item.stringToDatesType(item.label));

                    ops.add(op.build());
                } else if (existingIdList.contains(item.identifier)) {
                    // found update this item
                    for (Item existing : existingItemList) {
                        if (item.identifier.equals(existing.identifier)) {
                            if (!item.equalValues(existing)) {

                                Log.e(this.getClass().getSimpleName(), "addEventUpdateOperations : update 1 " + item.toString());
                                Log.e(this.getClass().getSimpleName(), "addEventUpdateOperations : update 1 existing " + existing.toString());

                                String[] queryArg = new String[]{item.identifier, ContactsContract.CommonDataKinds.Event.CONTENT_ITEM_TYPE};

                                op = ContentProviderOperation.newUpdate(ContactsContract.Data.CONTENT_URI)
                                        .withSelection(queryCommon, queryArg)
                                        .withValue(CommonDataKinds.Event.LABEL, item.label)
                                        .withValue(CommonDataKinds.Event.START_DATE, item.value)
                                        .withValue(CommonDataKinds.Event.TYPE, Item.stringToDatesType(item.label));

                                ops.add(op.build());
                            }
                            existingIdList.remove(item.identifier);
                        }
                    }
                }
            }

            if (existingIdList.size() > 0) {
                // remove all ids
                for (String id : existingIdList) {

                    Log.e(this.getClass().getSimpleName(), "addEventUpdateOperations : remove 2 " + id);

                    String[] queryArg = new String[]{id, ContactsContract.CommonDataKinds.Event.CONTENT_ITEM_TYPE};
                    op = ContentProviderOperation.newDelete(ContactsContract.Data.CONTENT_URI)
                            .withSelection(queryCommon, queryArg);
                    ops.add(op.build());
                }
            }
        }
    }
}
