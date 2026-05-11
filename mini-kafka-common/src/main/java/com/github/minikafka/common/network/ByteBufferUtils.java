package com.github.minikafka.common.network;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class ByteBufferUtils {

    private ByteBufferUtils() {}

    public static void writeString(ByteBuffer buffer, String value) {
        if (value == null) {
            buffer.putShort((short) -1);
        } else {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            buffer.putShort((short) bytes.length);
            buffer.put(bytes);
        }
    }

    public static String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        if (length < 0) return null;
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static void writeBytes(ByteBuffer buffer, byte[] value) {
        if (value == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(value.length);
            buffer.put(value);
        }
    }

    public static byte[] readBytes(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0) return null;
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    public static int sizeOfString(String value) {
        if (value == null) return 2;
        return 2 + value.getBytes(StandardCharsets.UTF_8).length;
    }

    public static int sizeOfBytes(byte[] value) {
        if (value == null) return 4;
        return 4 + value.length;
    }
}
