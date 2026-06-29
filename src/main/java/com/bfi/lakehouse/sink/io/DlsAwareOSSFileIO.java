package com.bfi.lakehouse.sink.io;

import org.apache.iceberg.aliyun.oss.OSSFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A {@link FileIO} wrapper that handles DLS-format OSS URLs by rewriting paths
 * before delegating to {@link OSSFileIO}.
 *
 * <p>DLS: {@code oss://bucket.region.oss-dls.aliyuncs.com/path}
 * → Standard: {@code oss://bucket/path}
 *
 * <p>Properties must include {@code oss.access-key-id} and {@code oss.access-key-secret}.
 */
public class DlsAwareOSSFileIO implements FileIO {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DlsAwareOSSFileIO.class);

    private static final Pattern DLS_PATTERN =
            Pattern.compile("^oss://([a-z0-9][a-z0-9-]+)\\.(.+\\.oss-dls\\.aliyuncs\\.com)/(.*)$");

    private OSSFileIO delegate;
    private Map<String, String> props;

    @Override
    public void initialize(Map<String, String> properties) {
        this.props = properties;

        // Resolve credentials from any known key
        String accessKeyId = firstNonEmpty(properties,
                "oss.access-key-id", "client.access-key-id", "access-key-id");
        String secretAccessKey = firstNonEmpty(properties,
                "oss.access-key-secret", "client.access-key-secret",
                "access-key-secret");

        LOG.info("DlsAwareOSSFileIO init — keyId={}... secret={}...",
                mask(accessKeyId), mask(secretAccessKey));

        // Build a new properties map with ALL known credential key variants
        // so the underlying OSSFileIO / AliyunProperties can find them
        Map<String, String> enriched = new HashMap<>(properties);
        if (accessKeyId != null) {
            enriched.put("client.access-key-id", accessKeyId);
        }
        if (secretAccessKey != null) {
            enriched.put("client.access-key-secret", secretAccessKey);
        }

        delegate = new OSSFileIO();
        delegate.initialize(enriched);
    }

    @Override
    public InputFile newInputFile(String path) {
        return delegate.newInputFile(rewrite(path));
    }

    @Override
    public OutputFile newOutputFile(String path) {
        return delegate.newOutputFile(rewrite(path));
    }

    @Override
    public void deleteFile(String path) {
        delegate.deleteFile(rewrite(path));
    }

    @Override
    public Map<String, String> properties() {
        return props;
    }

    @Override
    public void close() {
        if (delegate != null) {
            delegate.close();
        }
    }

    /**
     * Rewrites DLS-format path to standard OSS format.
     * {@code oss://my-bucket.ap-southeast-5.oss-dls.aliyuncs.com/path}
     * → {@code oss://my-bucket/path}
     */
    private String rewrite(String path) {
        Matcher m = DLS_PATTERN.matcher(path);
        if (m.matches()) {
            String bucket = m.group(1);
            String objectPath = m.group(3);
            String rewritten = "oss://" + bucket + "/" + objectPath;
            LOG.debug("DLS rewrite: {} → {}", path, rewritten);
            return rewritten;
        }
        return path;
    }

    private static String firstNonEmpty(Map<String, String> props, String... keys) {
        for (String key : keys) {
            String val = props.get(key);
            if (val != null && !val.isEmpty()) {
                return val;
            }
        }
        return null;
    }

    private static String mask(String val) {
        if (val == null) return "NULL";
        if (val.length() <= 4) return "****";
        return val.substring(0, 4) + "****";
    }
}
