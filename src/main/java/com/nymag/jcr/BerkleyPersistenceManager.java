package com.nymag.jcr;

import com.sleepycat.je.*;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.fs.BasedFileSystem;
import org.apache.jackrabbit.core.fs.FileSystem;
import org.apache.jackrabbit.core.fs.FileSystemResource;
import org.apache.jackrabbit.core.fs.local.LocalFileSystem;
import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.id.PropertyId;
import org.apache.jackrabbit.core.persistence.PMContext;
import org.apache.jackrabbit.core.persistence.bundle.AbstractBundlePersistenceManager;
import org.apache.jackrabbit.core.persistence.util.*;
import org.apache.jackrabbit.core.state.ItemStateException;
import org.apache.jackrabbit.core.state.NoSuchItemStateException;
import org.apache.jackrabbit.core.state.NodeReferences;
import org.apache.jackrabbit.core.util.StringIndex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class BerkleyPersistenceManager extends AbstractBundlePersistenceManager {

    private Database db;
    private BundleBinding binding;
    private StringIndex nsIndex;
    private StringIndex nameIndex;
    private BLOBStore blobStore;
    private FileSystem itemFs;
    private int blobFSBlockSize;

    private static final Logger log = LoggerFactory.getLogger(BerkleyPersistenceManager.class);
    @Override
    public void init(PMContext context) throws Exception {
        super.init(context);
                // create item fs
        itemFs = new BasedFileSystem(context.getFileSystem(), "items");

        // create correct blob store
        if (useLocalFsBlobStore()) {
            LocalFileSystem blobFS = new LocalFileSystem();
            blobFS.setRoot(new File(context.getHomeDir(), "blobs"));
            blobFS.init();
            blobStore = new BerkleyPersistenceManager.FSBlobStore(blobFS);
        } else {
            blobStore = new BerkleyPersistenceManager.FSBlobStore(itemFs);
        }
        try {
            binding = new BundleBinding(new ErrorHandling(),
                    this.getBlobStore(),
                     getNsIndex(),
                    getNameIndex(),
                    context.getDataStore());

            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            String dbPath = context.getHomeDir() + "/bdb";
            if (!context.getFileSystem().exists(dbPath)) {
                log.info("creating db");
                context.getFileSystem().createFolder(dbPath);
            }

            Environment myDbEnvironment = new Environment(new File(dbPath), envConfig);

            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            db = myDbEnvironment.openDatabase(null, "sampleDatabase", dbConfig);
        } catch (DatabaseException dbe) {
            throw new Exception(dbe.getMessage(), dbe);
        }
    }

        public boolean useLocalFsBlobStore() {
        return blobFSBlockSize == 0;
    }

    @Override
    protected NodePropBundle loadBundle(NodeId id) throws ItemStateException {

        DatabaseEntry key = new DatabaseEntry(id.getRawBytes());
        DatabaseEntry data = new DatabaseEntry();
        OperationStatus status = db.get(null, key, data, null);

        switch (status){
            case SUCCESS:
                log.info("successfully retrieved bundle {}", id);
                return byteArrayToBundle(data.getData(), id);

            case KEYEXIST:
                log.info("key exists for bundle {}", id);
                return null;
            default:
                log.info("returning null for non-existent key {}", id);
                return null;
        }
    }
    private NodePropBundle byteArrayToBundle(byte[] data, NodeId id) throws ItemStateException{
        ByteArrayInputStream in = null;
        try {
            in = new ByteArrayInputStream(data);
            return binding.readBundle(in,id);
        } catch (IOException e) {
            throw new ItemStateException(e.getMessage(), e);

        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    @Override
    protected void storeBundle(NodePropBundle bundle) throws ItemStateException {
        log.info("request to store bundle {}", bundle.getId());

        ByteArrayOutputStream out = null;
        try {
            out = new ByteArrayOutputStream();
            binding.writeBundle(out, bundle);
            db.put(null, new DatabaseEntry(bundle.getId().getRawBytes()), new DatabaseEntry(out.toByteArray()));

        } catch (IOException e) {
            throw new ItemStateException(e.getMessage(), e);
        }
        finally {
            IOUtils.closeQuietly(out);
        }

    }

    @Override
    protected void destroyBundle(NodePropBundle bundle) throws ItemStateException {
        log.info("request to destroy bundle {}", bundle.getId());
    }

    @Override
    protected void destroy(NodeReferences refs) throws ItemStateException {
        log.info("request to destroy references to target {}", refs.getTargetId());
    }

    @Override
    protected void store(NodeReferences refs) throws ItemStateException {
        log.info("request to store references to target {}", refs.getTargetId());
    }

    @Override
    protected BLOBStore getBlobStore() {
        return blobStore;
    }

    @Override
    public Iterable<NodeId> getAllNodeIds(NodeId after, int maxCount) throws ItemStateException, RepositoryException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NodeReferences loadReferencesTo(NodeId id) throws NoSuchItemStateException, ItemStateException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean existsReferencesTo(NodeId targetId) throws ItemStateException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
    @Override
    public void close() throws Exception {
        super.close();
        db.close();
    }
        /**
     * Returns the namespace index
     * @return the namespace index
     * @throws IllegalStateException if an error occurs.
     */
    public StringIndex getNsIndex() {
        try {
            if (nsIndex == null) {
                // load name and ns index
                FileSystemResource nsFile = new FileSystemResource(context.getFileSystem(), RES_NS_INDEX);
                if (nsFile.exists()) {
                    nsIndex = new FileBasedIndex(nsFile);
                } else {
                    nsIndex = (StringIndex) context.getNamespaceRegistry();
                }
            }
            return nsIndex;
        } catch (Exception e) {
            IllegalStateException e2 = new IllegalStateException("Unable to create nsIndex.");
            e2.initCause(e);
            throw e2;
        }
    }

    /**
     * Returns the local name index
     * @return the local name index
     * @throws IllegalStateException if an error occurs.
     */
    public StringIndex getNameIndex() {
        try {
            if (nameIndex == null) {
                nameIndex = new FileBasedIndex(new FileSystemResource(
                        context.getFileSystem(), RES_NAME_INDEX));
            }
            return nameIndex;
        } catch (Exception e) {
            IllegalStateException e2 = new IllegalStateException("Unable to create nameIndex.");
            e2.initCause(e);
            throw e2;
        }
    }
        protected static interface CloseableBLOBStore extends BLOBStore {
        void close();
    }
        private class FSBlobStore extends FileSystemBLOBStore implements BerkleyPersistenceManager.CloseableBLOBStore {

        private FileSystem fs;

        public FSBlobStore(FileSystem fs) {
            super(fs);
            this.fs = fs;
        }

        public String createId(PropertyId id, int index) {
            return buildBlobFilePath(null, id, index).toString();
        }

        public void close() {
            try {
                fs.close();
                fs = null;
            } catch (Exception e) {
                // ignore
            }
        }
    }
}
