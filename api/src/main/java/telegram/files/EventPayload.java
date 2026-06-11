package telegram.files;

public record EventPayload(int type, String code, Object data, long timestamp) {
    public static final int TYPE_ERROR = -1;

    public static final int TYPE_AUTHORIZATION = 1;

    public static final int TYPE_METHOD_RESULT = 2;

    public static final int TYPE_FILE = 3;

    public static final int TYPE_FILE_DOWNLOAD = 4;

    public static final int TYPE_FILE_STATUS = 5;

    public static final int TYPE_CONNECTION = 6;

    public static EventPayload build(int type, Object data) {
        return new EventPayload(type, null, data, System.currentTimeMillis());
    }

    public static EventPayload build(int type, String code, Object data) {
        return new EventPayload(type, code, data, System.currentTimeMillis());
    }
}
