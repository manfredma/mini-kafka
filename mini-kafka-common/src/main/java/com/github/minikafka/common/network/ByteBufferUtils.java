package com.github.minikafka.common.network;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * {@link ByteBuffer} 读写工具类，封装 Kafka 协议中常用的基础数据类型序列化/反序列化操作。
 *
 * <p>对应 Kafka 原版 {@code org.apache.kafka.common.utils.ByteUtils} 及协议层编解码辅助方法。
 *
 * <p>支持两种变长类型：
 * <ul>
 *   <li><b>NULLABLE_STRING</b>：2 字节有符号长度前缀 + UTF-8 字节；长度为 -1 表示 null。</li>
 *   <li><b>NULLABLE_BYTES</b>：4 字节有符号长度前缀 + 原始字节；长度为 -1 表示 null。</li>
 * </ul>
 *
 * <p>关键设计决策：
 * <ul>
 *   <li>工具类，私有构造器，所有方法均为静态，不可实例化。</li>
 *   <li>null 值在协议层使用负长度（-1）编码，读取时原样还原为 null，
 *       与 Kafka 协议规范中 NULLABLE_STRING / NULLABLE_BYTES 的语义一致。</li>
 *   <li>字符串统一使用 UTF-8 编码，与 Kafka 协议规范保持一致。</li>
 * </ul>
 */
public final class ByteBufferUtils {

    private ByteBufferUtils() {}

    /**
     * 按 Kafka NULLABLE_STRING 格式将字符串写入 {@code buffer}。
     *
     * <p>格式：2 字节有符号长度（{@code short}）+ UTF-8 编码字节。
     * 当 {@code value} 为 null 时，写入长度 {@code -1}，不写入任何字节内容。
     *
     * @param buffer 目标 ByteBuffer，须有足够的剩余容量
     * @param value  待写入的字符串，允许为 null
     */
    public static void writeString(ByteBuffer buffer, String value) {
        if (value == null) {
            buffer.putShort((short) -1);
        } else {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            buffer.putShort((short) bytes.length);
            buffer.put(bytes);
        }
    }

    /**
     * 从 {@code buffer} 当前位置按 Kafka NULLABLE_STRING 格式读取字符串。
     *
     * <p>先读取 2 字节有符号长度：若长度为负数，返回 null；否则读取对应字节数并以 UTF-8 解码。
     *
     * @param buffer 源 ByteBuffer，position 需指向长度字段起始位置
     * @return 解码后的字符串；若长度字段为负数则返回 null
     */
    public static String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        if (length < 0) return null;
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 按 Kafka NULLABLE_BYTES 格式将字节数组写入 {@code buffer}。
     *
     * <p>格式：4 字节有符号长度（{@code int}）+ 原始字节。
     * 当 {@code value} 为 null 时，写入长度 {@code -1}，不写入任何字节内容。
     *
     * @param buffer 目标 ByteBuffer，须有足够的剩余容量
     * @param value  待写入的字节数组，允许为 null
     */
    public static void writeBytes(ByteBuffer buffer, byte[] value) {
        if (value == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(value.length);
            buffer.put(value);
        }
    }

    /**
     * 从 {@code buffer} 当前位置按 Kafka NULLABLE_BYTES 格式读取字节数组。
     *
     * <p>先读取 4 字节有符号长度：若长度为负数，返回 null；否则读取对应字节数。
     *
     * @param buffer 源 ByteBuffer，position 需指向长度字段起始位置
     * @return 读取到的字节数组；若长度字段为负数则返回 null
     */
    public static byte[] readBytes(ByteBuffer buffer) {
        int length = buffer.getInt();
        if (length < 0) return null;
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }

    /**
     * 计算字符串按 Kafka NULLABLE_STRING 格式序列化后占用的字节数。
     *
     * <p>null 值仅占 2 字节（长度字段本身）；非 null 值占 2 + UTF-8 编码字节数。
     *
     * @param value 待计算的字符串，允许为 null
     * @return 序列化字节数，最小为 2（null 情况）
     */
    public static int sizeOfString(String value) {
        if (value == null) return 2;
        return 2 + value.getBytes(StandardCharsets.UTF_8).length;
    }

    /**
     * 计算字节数组按 Kafka NULLABLE_BYTES 格式序列化后占用的字节数。
     *
     * <p>null 值仅占 4 字节（长度字段本身）；非 null 值占 4 + 数组长度。
     *
     * @param value 待计算的字节数组，允许为 null
     * @return 序列化字节数，最小为 4（null 情况）
     */
    public static int sizeOfBytes(byte[] value) {
        if (value == null) return 4;
        return 4 + value.length;
    }
}
