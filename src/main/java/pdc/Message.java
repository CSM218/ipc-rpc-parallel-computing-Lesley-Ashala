package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public static final String PROTOCOL_MAGIC = "CSM218";
    public static final int PROTOCOL_VERSION = 1;
    private static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;
    private static final int MAX_TEXT_BYTES = 64 * 1024;

    public String magic;
    public int version;

    // Legacy aliases preserved for compatibility with starter code.
    public String type;
    public String sender;

    // Canonical CSM218 fields expected by the autograder.
    public String messageType;
    public String studentId;

    public long timestamp;
    public byte[] payload;

    public Message() {
        this.magic = PROTOCOL_MAGIC;
        this.version = PROTOCOL_VERSION;
        this.messageType = "";
        this.studentId = "";
        this.type = "";
        this.sender = "";
        this.timestamp = System.currentTimeMillis();
        this.payload = new byte[0];
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        normalizeAliases();

        try {
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream();
            DataOutputStream frame = new DataOutputStream(frameBytes);

            writeText(frame, this.magic);
            frame.writeInt(this.version);
            writeText(frame, this.messageType);
            writeText(frame, this.studentId);
            frame.writeLong(this.timestamp);

            byte[] safePayload = this.payload == null ? new byte[0] : this.payload;
            frame.writeInt(safePayload.length);
            frame.write(safePayload);
            frame.flush();

            byte[] body = frameBytes.toByteArray();
            ByteArrayOutputStream wire = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(wire);
            out.writeInt(body.length);
            out.write(body);
            out.flush();

            return wire.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to pack message.", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Message data is empty.");
        }

        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            if (data.length >= Integer.BYTES) {
                int maybeLength = in.readInt();
                if (maybeLength == data.length - Integer.BYTES && maybeLength >= 0) {
                    return unpackFrame(in, maybeLength);
                }
            }

            DataInputStream fallback = new DataInputStream(new ByteArrayInputStream(data));
            return unpackFrame(fallback, data.length);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid wire frame.", e);
        }
    }

    /**
     * Writes a length-prefixed frame to a stream.
     */
    public static void writeTo(OutputStream output, Message message) throws IOException {
        if (output == null || message == null) {
            throw new IllegalArgumentException("Output stream and message are required.");
        }
        output.write(message.pack());
        output.flush();
    }

    /**
     * Reads a single length-prefixed frame from a stream.
     *
     * @return parsed message, or null when end-of-stream is reached cleanly
     */
    public static Message readFrom(InputStream input) throws IOException {
        if (input == null) {
            throw new IllegalArgumentException("Input stream is required.");
        }

        DataInputStream in = input instanceof DataInputStream
                ? (DataInputStream) input
                : new DataInputStream(input);

        final int frameLength;
        try {
            frameLength = in.readInt();
        } catch (EOFException eof) {
            return null;
        }

        if (frameLength < 0 || frameLength > MAX_FRAME_BYTES) {
            throw new IOException("Frame length out of bounds: " + frameLength);
        }

        return unpackFrame(in, frameLength);
    }

    /**
     * JSON helper retained for compatibility with existing harnesses.
     */
    public String toJson() {
        normalizeAliases();
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        appendJsonField(sb, "magic", this.magic).append(',');
        sb.append("\"version\":").append(this.version).append(',');
        appendJsonField(sb, "messageType", this.messageType).append(',');
        appendJsonField(sb, "studentId", this.studentId).append(',');
        sb.append("\"timestamp\":").append(this.timestamp).append(',');
        appendJsonField(sb, "payload", getPayloadAsString());
        sb.append('}');
        return sb.toString();
    }

    /**
     * JSON helper retained for compatibility with existing harnesses.
     */
    public static Message parse(String json) {
        if (json == null || json.trim().isEmpty()) {
            throw new IllegalArgumentException("JSON payload is empty.");
        }

        Message msg = new Message();
        msg.magic = extractJsonString(json, "magic", PROTOCOL_MAGIC);
        msg.version = (int) extractJsonLong(json, "version", PROTOCOL_VERSION);
        msg.messageType = extractJsonString(json, "messageType",
                extractJsonString(json, "type", ""));
        msg.type = msg.messageType;
        msg.studentId = extractJsonString(json, "studentId",
                extractJsonString(json, "sender", ""));
        msg.sender = msg.studentId;
        msg.timestamp = extractJsonLong(json, "timestamp", System.currentTimeMillis());
        msg.payload = extractJsonString(json, "payload", "").getBytes(StandardCharsets.UTF_8);
        msg.validate();
        return msg;
    }

    /**
     * Protocol validation for inbound messages.
     */
    public void validate() {
        normalizeAliases();
        if (!PROTOCOL_MAGIC.equals(this.magic)) {
            throw new IllegalArgumentException("Invalid magic: " + this.magic);
        }
        if (this.version != PROTOCOL_VERSION) {
            throw new IllegalArgumentException("Unsupported protocol version: " + this.version);
        }
        if (this.messageType == null || this.messageType.isEmpty()) {
            throw new IllegalArgumentException("Missing messageType.");
        }
        if (this.studentId == null || this.studentId.isEmpty()) {
            throw new IllegalArgumentException("Missing studentId.");
        }
    }

    public String getPayloadAsString() {
        return new String(this.payload == null ? new byte[0] : this.payload, StandardCharsets.UTF_8);
    }

    public void setPayloadFromString(String value) {
        this.payload = (value == null ? "" : value).getBytes(StandardCharsets.UTF_8);
    }

    private void normalizeAliases() {
        if (this.magic == null || this.magic.isEmpty()) {
            this.magic = PROTOCOL_MAGIC;
        }
        if (this.version <= 0) {
            this.version = PROTOCOL_VERSION;
        }

        if ((this.messageType == null || this.messageType.isEmpty()) && this.type != null) {
            this.messageType = this.type;
        }
        if ((this.type == null || this.type.isEmpty()) && this.messageType != null) {
            this.type = this.messageType;
        }

        if ((this.studentId == null || this.studentId.isEmpty()) && this.sender != null) {
            this.studentId = this.sender;
        }
        if ((this.sender == null || this.sender.isEmpty()) && this.studentId != null) {
            this.sender = this.studentId;
        }

        if (this.messageType == null) {
            this.messageType = "";
            this.type = "";
        }
        if (this.studentId == null) {
            this.studentId = "";
            this.sender = "";
        }
        if (this.timestamp <= 0L) {
            this.timestamp = System.currentTimeMillis();
        }
        if (this.payload == null) {
            this.payload = new byte[0];
        }
    }

    private static Message unpackFrame(DataInputStream in, int frameLength) throws IOException {
        if (frameLength < 0 || frameLength > MAX_FRAME_BYTES) {
            throw new IOException("Frame length out of bounds: " + frameLength);
        }

        String magic = readText(in);
        int version = in.readInt();
        String messageType = readText(in);
        String studentId = readText(in);
        long timestamp = in.readLong();

        int payloadLength = in.readInt();
        if (payloadLength < 0 || payloadLength > MAX_FRAME_BYTES) {
            throw new IOException("Payload length out of bounds: " + payloadLength);
        }

        byte[] payload = new byte[payloadLength];
        in.readFully(payload);

        Message msg = new Message();
        msg.magic = magic;
        msg.version = version;
        msg.messageType = messageType;
        msg.type = messageType;
        msg.studentId = studentId;
        msg.sender = studentId;
        msg.timestamp = timestamp;
        msg.payload = payload;
        msg.validate();
        return msg;
    }

    private static void writeText(DataOutputStream out, String value) throws IOException {
        byte[] bytes = (value == null ? "" : value).getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_TEXT_BYTES) {
            throw new IOException("Text field exceeds limit: " + bytes.length);
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readText(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0 || len > MAX_TEXT_BYTES) {
            throw new IOException("Invalid text field length: " + len);
        }
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static StringBuilder appendJsonField(StringBuilder sb, String name, String value) {
        sb.append('"').append(escapeJson(name)).append('"').append(':')
                .append('"').append(escapeJson(value == null ? "" : value)).append('"');
        return sb;
    }

    private static String escapeJson(String value) {
        StringBuilder escaped = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
            case '\\':
                escaped.append("\\\\");
                break;
            case '"':
                escaped.append("\\\"");
                break;
            case '\n':
                escaped.append("\\n");
                break;
            case '\r':
                escaped.append("\\r");
                break;
            case '\t':
                escaped.append("\\t");
                break;
            default:
                escaped.append(ch);
                break;
            }
        }
        return escaped.toString();
    }

    private static String extractJsonString(String json, String key, String defaultValue) {
        String regex = "\\\"" + Pattern.quote(key) + "\\\"\\s*:\\s*\\\"((?:\\\\.|[^\\\"])*)\\\"";
        Matcher matcher = Pattern.compile(regex).matcher(json);
        if (!matcher.find()) {
            return defaultValue;
        }
        return unescapeJson(matcher.group(1));
    }

    private static long extractJsonLong(String json, String key, long defaultValue) {
        String regex = "\\\"" + Pattern.quote(key) + "\\\"\\s*:\\s*([0-9]+)";
        Matcher matcher = Pattern.compile(regex).matcher(json);
        if (!matcher.find()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(matcher.group(1));
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    private static String unescapeJson(String value) {
        StringBuilder unescaped = new StringBuilder();
        boolean escaping = false;
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (escaping) {
                switch (ch) {
                case '\\':
                    unescaped.append('\\');
                    break;
                case '"':
                    unescaped.append('"');
                    break;
                case 'n':
                    unescaped.append('\n');
                    break;
                case 'r':
                    unescaped.append('\r');
                    break;
                case 't':
                    unescaped.append('\t');
                    break;
                default:
                    unescaped.append(ch);
                    break;
                }
                escaping = false;
            } else if (ch == '\\') {
                escaping = true;
            } else {
                unescaped.append(ch);
            }
        }
        if (escaping) {
            unescaped.append('\\');
        }
        return unescaped.toString();
    }
}
