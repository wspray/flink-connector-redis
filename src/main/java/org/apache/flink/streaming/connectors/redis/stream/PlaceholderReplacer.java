package org.apache.flink.streaming.connectors.redis.stream;

import org.apache.commons.text.StringSubstitutor;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlaceholderReplacer {

    private static final Logger LOG = LoggerFactory.getLogger(PlaceholderReplacer.class);

    private static final String DEFAULT_VAR_START = "\\{";
    private static final String DEFAULT_VAR_END = "}";
    private static final char DEFAULT_ESCAPE = '$';
    private static final String DEFAULT_VAR_DEFAULT = ":";

    public static String replaceByTag(Row row, String template) {
        if (template == null || template.isEmpty() || row == null) {
            return template;
        }

        try {
            // 模板里没有出现“{...}”占位符 -> 把整串当字段名去取值
            if (!(template.contains(DEFAULT_VAR_START) && template.contains(DEFAULT_VAR_END))) {
                Object val = row.getField(template);
                return val == null ? template : val.toString();
            }

            StringSubstitutor substitutor = new StringSubstitutor(
                    key -> {
                        Object v = row.getField(key);
                        return v == null ? null : v.toString();
                    },
                    DEFAULT_VAR_START,
                    DEFAULT_VAR_END,
                    DEFAULT_ESCAPE,
                    DEFAULT_VAR_DEFAULT);

            // 保留未匹配占位符：模板里出现未知变量时，保留原占位符并不直接抛异常
            substitutor.setEnableUndefinedVariableException(false);
            return substitutor.replace(template);
        } catch (Exception e) {
            LOG.error("PlaceholderReplacer replaceByTag exception: ", e);
            return template;
        }
    }

//    public static void main(String[] args) {
//        Row row = Row.withNames();
//        row.setField("name", "Alice");
//        row.setField("id", "1001");
//        System.out.println(replaceByTag(row, "hello \\{name} $\\{name}, your id is \\{id2:123}."));
//        // 输出: hello Alice \{name}, your id is {id2:123}.
//        System.out.println(replaceByTag(row, "name"));
//    }
}