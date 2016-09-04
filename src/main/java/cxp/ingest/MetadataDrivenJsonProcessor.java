package cxp.ingest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemProcessor;

import java.util.Collections;
import java.util.List;

/**
 * Created by markmo on 7/04/15.
 */
public class MetadataDrivenJsonProcessor implements ItemProcessor<String, List<CustomerEvent>> {

    private static final Log log = LogFactory.getLog(MetadataDrivenJsonProcessor.class);

    MetadataDrivenItemTransformer transformer;

    @Override
    public List<CustomerEvent> process(String item) throws Exception {
        if (item == null || item.isEmpty()) {
            return Collections.emptyList();
        }
        return transformer.<String>transform(item);
    }

    public void setTransformer(MetadataDrivenItemTransformer transformer) {
        this.transformer = transformer;
    }
}