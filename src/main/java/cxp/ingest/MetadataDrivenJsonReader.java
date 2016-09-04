package cxp.ingest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by markmo on 7/04/15.
 */
public class MetadataDrivenJsonReader extends FlatFileItemScanner<String> {

    private static final Log log = LogFactory.getLog(MetadataDrivenJsonReader.class);

    private static final String TEST_PATTERN = "/test/";

    MetadataProvider metadataProvider;

    public MetadataDrivenJsonReader() {
        super();
        setRowDelimiter("}");
        setRecordSeparatorPolicy(new MyJsonRecordSeparatorPolicy());
        if (log.isDebugEnabled()) {
            setLineMapper(new LineMapper<String>() {
                @Override
                public String mapLine(String line, int lineNumber) throws Exception {
                    log.debug("--------------------------------------------------------------------------------");
                    log.debug(line);
                    return line;
                }
            });
        } else {
            setLineMapper(new PassThroughLineMapper());
        }
    }

    @Override
    public void setResource(Resource resource) {
        super.setResource(resource);
        metadataProvider.setFilename(resource.getFilename());
        final FileDataset fileDataset = metadataProvider.getFileDataset();
        try {
            Pattern p = Pattern.compile(TEST_PATTERN);
            String absolutePath = resource.getFile().getAbsolutePath();
            Matcher matcher = p.matcher(absolutePath);
            metadataProvider.setTest(matcher.find());
            metadataProvider.startJob();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }

        if (fileDataset == null) {
            log.warn("No dataset found for '" + resource.getFilename() + "'");
            throw new RuntimeException("No dataset found for '" + resource.getFilename() + "'");
        }
    }

    public void setMetadataProvider(MetadataProvider metadataProvider) {
        this.metadataProvider = metadataProvider;
    }
}
