package com.kongbig.sparkproject.util;

/**
 * Describe: 自定义字符串工具类
 * Author:   kongbig
 * Data:     2018/1/30.
 */
public class CustomStringUtils {

    /**
     * 截断字符串两侧的逗号
     *
     * @param str 字符串
     * @return 字符串
     */
    public static String trimComma(String str) {
        if (str.startsWith(",")) {
            str = str.substring(1);
        }
        if (str.endsWith(",")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }

    /**
     * 补全两位数字
     *
     * @param str
     * @return
     */
    public static String fulfuill(String str) {
        if (str.length() == 2) {
            return str;
        } else {
            return "0" + str;
        }
    }

    /**
     * 从拼接的字符串中提取字段
     *
     * @param str       字符串
     * @param delimiter 分隔符
     * @param field     字段
     * @return 字符值
     */
    public static String getFieldFromConcatString(String str, String delimiter, String field) {
        String[] fields = str.split(delimiter);
        for (String concatField : fields) {
            if (concatField.split("=").length == 2) {
                String fieldName = concatField.split("=")[0];
                String fieldsValue = concatField.split("=")[1];
                if (fieldName.equals(field)) {
                    return fieldsValue;
                }
            }
        }
        return null;
    }

    /**
     * 拼接的字符串中给字段设置值
     *
     * @param str           字符串
     * @param delimiter     分隔符
     * @param field         字段名
     * @param newFieldValue 新的field值
     * @return 字段值
     */
    public static String setFieldInConcatString(String str, String delimiter, String field, String newFieldValue) {
        String[] fields = str.split(delimiter);

        for (int i = 0; i < fields.length; i++) {
            String fieldName = fields[i].split("=")[0];
            if (fieldName.equals(field)) {
                String concatField = fieldName + "=" + newFieldValue;
                fields[i] = concatField;
                break;
            }
        }

        StringBuffer buffer = new StringBuffer("");
        for (int i = 0; i < fields.length; i++) {
            buffer.append(fields[i]);
            if (i < fields.length - 1) {
                buffer.append("|");
            }
        }

        return buffer.toString();
    }

}
