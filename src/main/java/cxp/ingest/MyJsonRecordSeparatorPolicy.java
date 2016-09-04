package cxp.ingest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.SimpleRecordSeparatorPolicy;
import org.springframework.util.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by markmo on 6/05/15.
 */
public class MyJsonRecordSeparatorPolicy extends SimpleRecordSeparatorPolicy {

    private static final Log log = LogFactory.getLog(MyJsonRecordSeparatorPolicy.class);

    private static final Pattern EOF = Pattern.compile("(}|])\\s*$");

    private static final Pattern EMPTY_RECORD = Pattern.compile("^\\s*]");

    /**
     * True if the line can be parsed to a JSON object.
     *
     * @see RecordSeparatorPolicy#isEndOfRecord(String)
     */
    @Override
    public boolean isEndOfRecord(String line) {
        Matcher m = EOF.matcher(line);
        if (!m.find()) {
            line += "}";
        }
        return StringUtils.countOccurrencesOf(line, "{") == StringUtils.countOccurrencesOf(line, "}") &&
                (line.trim().endsWith("}") || line.trim().endsWith("},") || line.trim().endsWith("]"));
    }

    @Override
    public String postProcess(String record) {
        Matcher matcher = EMPTY_RECORD.matcher(record);
        if (matcher.find()) {
            return "";
        }
        Matcher m = EOF.matcher(record);
        if (!m.find()) {
            record += "}";
        }
        int start;
        if ((record.startsWith("[") || record.startsWith(",")) &&
                (start = record.indexOf('{')) > 0) {
            record = record.substring(start);
        }
        if (record.endsWith(",")) {
            record = record.substring(0, record.length() - 1);
        }
        return record;
    }
}
