package org.apache.flink.streaming.connectors.redis.stream;

import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PlaceholderReplacer {

    private static final Logger LOG = LoggerFactory.getLogger(PlaceholderReplacer.class);

    private static final Pattern PATTERN = Pattern.compile("\\{([^}]+)}");

    public static String replaceByTag(Row row, String template) {
        try {
            if (template == null || template.isEmpty() || row == null) {
                return template;
            }

            // 没有{}占位符，则整体当做name从row获取
            if (!PATTERN.matcher(template).find()) {
                Object fieldValue = row.getField(template);
                return fieldValue == null ? template : fieldValue.toString();
            }

            Matcher matcher = PATTERN.matcher(template);
            StringBuffer sb = new StringBuffer();
            while (matcher.find()) {
                try {
                    String key = matcher.group(1);
                    Object value = row.getField(key);
                    // 如果 map 中没有对应 key，则保留原占位符
                    matcher.appendReplacement(sb,
                            Matcher.quoteReplacement(
                                    value == null ? matcher.group() : String.valueOf(value)));
                } catch (Exception e) {
                    LOG.error("Redis replaceByTag appendReplacement exception: ", e);
                }
            }
            matcher.appendTail(sb);
            return sb.toString();
        } catch (Exception e) {
            LOG.error("Redis replaceByTag exception: ", e);
            return template;
        }
    }

//    public static void main(String[] args) {
//        Row row = Row.withNames();
//        row.setField("name", "Alice");
//        row.setField("id", "1001");
//        System.out.println(replaceByTag(row, "hello {name}, your id is {id}."));
//        // 输出: hello Alice, your id is 1001.
//        System.out.println(replaceByTag(row, "name"));
//    }
}