/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   24.04.2018 ("Mareike Hoeger, KNIME GmbH, Konstanz, Germany"): created
 */
package org.knime.orc;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.bigdata.commons.hadoop.ConfigurationFactory;
import org.knime.cloud.aws.s3.filehandler.S3Connection;
import org.knime.cloud.core.file.CloudRemoteFile;
import org.knime.cloud.core.util.port.CloudConnectionInformation;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowIterator;
import org.knime.core.data.RowKey;
import org.knime.core.data.blob.BinaryObjectDataCell;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.container.BlobSupportDataRow;
import org.knime.core.data.container.storage.AbstractTableStoreReader.TableStoreCloseableRowIterator;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DefaultCellIterator;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.data.time.localdatetime.LocalDateTimeCellFactory;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.KnimeEncryption;
import org.knime.orc.types.OrcStringTypeFactory.OrcStringType;
import org.knime.orc.types.OrcType;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

/**
 * Reader that reads ORC files into a KNIME data table.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class OrcKNIMEReader {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(OrcKNIMEReader.class);

    private RemoteFile<Connection> m_file;

    private boolean m_isReadRowKey;

    private int m_batchSize;

    private OrcType<?>[] m_columnReaders;

    private DataTableSpec m_tableSpec;

    private Queue<Reader> m_readers;

    // Use a map for now, this will be changed once the type conversion framework is used
    private Map<Category, DataType> m_types = new EnumMap<>(Category.class);

    private final ExecutionContext m_exec;

    {
        m_types.put(Category.INT, IntCell.TYPE);
        m_types.put(Category.DOUBLE, DoubleCell.TYPE);
        m_types.put(Category.LONG, LongCell.TYPE);
        m_types.put(Category.STRING, StringCell.TYPE);
        m_types.put(Category.BINARY, BinaryObjectDataCell.TYPE);
        m_types.put(Category.BOOLEAN, BooleanCell.TYPE);
        m_types.put(Category.BYTE, BinaryObjectDataCell.TYPE);
        m_types.put(Category.CHAR, StringCell.TYPE);
        m_types.put(Category.TIMESTAMP, LocalDateTimeCellFactory.TYPE);
        m_types.put(Category.SHORT, IntCell.TYPE);
        m_types.put(Category.FLOAT, DoubleCell.TYPE);
        m_types.put(Category.DATE, LongCell.TYPE);
        m_types.put(Category.DECIMAL, BinaryObjectDataCell.TYPE);
        m_types.put(Category.VARCHAR, StringCell.TYPE);
        m_types.put(Category.MAP, BinaryObjectDataCell.TYPE);
        m_types.put(Category.STRUCT, BinaryObjectDataCell.TYPE);
        m_types.put(Category.UNION, BinaryObjectDataCell.TYPE);
    }

    /**
     * KNIME REader, that reads ORC Files into DataRows.
     *
     * @param file the file or directory to read from
     * @param isReadRowKey if the row key has to be read
     * @param batchSize the batch size for reading
     * @param exec the execution context
     * @throws Exception thrown if files can not be listed, if reader can not be created, or schemas of files in a
     *             directory do not match.
     *
     */
    public OrcKNIMEReader(final RemoteFile<Connection> file, final boolean isReadRowKey, final int batchSize,
        final ExecutionContext exec) throws Exception {
        super();
        m_exec = exec;
        m_file = file;
        m_isReadRowKey = isReadRowKey;
        m_batchSize = batchSize;
        m_readers = new ArrayDeque<>();

        if (m_file.isDirectory()) {
            RemoteFile<Connection>[] fileList = m_file.listFiles();
            List<TypeDescription> schemas = new ArrayList<>();

            for (RemoteFile<Connection> remotefile : fileList) {
                Reader reader = createORCReader(remotefile);
                if (reader != null) {
                    m_readers.add(reader);
                    exec.setProgress("Retrieving schema for file " + remotefile.getName());
                    exec.checkCanceled();
                    schemas.add(reader.getSchema());
                }
            }
            if (!schemas.isEmpty()) {
                checkSchemas(schemas);
            }
        } else {
            Reader reader = createORCReader(m_file);
            m_readers.add(reader);
        }
        if (m_readers.isEmpty()) {
            return;
        }

        loadMetaInfoBeforeRead(m_readers.peek());
    }

    private void checkSchemas(final List<TypeDescription> schemas) {
        TypeDescription orcSchema;
        orcSchema = schemas.get(0);
        for (int i = 1; i < schemas.size(); i++) {
            if (orcSchema.compareTo(schemas.get(i)) != 0) {
                throw new OrcReadException("Schemas of input files do not match.");
            }
        }
    }

    private Reader createORCReader(final RemoteFile<Connection> remotefile) throws Exception {
        Configuration conf = new Configuration();
        createConfig(remotefile, conf);
        Path readpath = new Path(remotefile.getURI());
        //ignore empty files.
        if (remotefile.getSize() > 0) {
            if (m_file.getConnection() instanceof S3Connection) {
                CloudRemoteFile<Connection> cloudcon = (CloudRemoteFile<Connection>)remotefile;
                readpath = new Path(new URI("s3n", cloudcon.getContainerName(), "/" + cloudcon.getBlobName(), null));
            }
            return OrcFile.createReader(readpath, OrcFile.readerOptions(conf));
        }
        return null;
    }

    private void createConfig(final RemoteFile<Connection> remotefile, final Configuration conf) throws Exception {

        if (remotefile.getConnectionInformation() == null) {
            return;
        }
        if (remotefile.getConnectionInformation().useKerberos()) {
            conf.addResource(ConfigurationFactory.createBaseConfigurationWithKerberosAuth());
        }
        if (remotefile.getConnectionInformation() instanceof CloudConnectionInformation) {
            String accessID;
            String secretKey;
            Thread.currentThread().setContextClassLoader(SnappyCodec.class.getClassLoader());
            if (((CloudConnectionInformation)remotefile.getConnectionInformation()).useKeyChain()) {
                DefaultAWSCredentialsProviderChain chain = new DefaultAWSCredentialsProviderChain();
                chain.getCredentials().getAWSSecretKey();
                accessID = chain.getCredentials().getAWSAccessKeyId();
                secretKey = chain.getCredentials().getAWSSecretKey();
            } else {
                accessID = remotefile.getConnectionInformation().getUser();
                secretKey = KnimeEncryption.decrypt(remotefile.getConnectionInformation().getPassword());
            }
            conf.set("fs.s3n.awsAccessKeyId", accessID);
            conf.set("fs.s3n.awsSecretAccessKey", secretKey);
        }
    }

    /**
     * Returns a RowIterator that iterates over the read DataRows.
     *
     * @return the RowIterator
     * @throws IOException thrown if reading errors occur
     */
    public RowIterator iterator() throws IOException {
        if (m_columnReaders == null) {
            throw new IOException(
                "No information for type deserialization loaded. Most likely an implementation error.");
        }
        return new DirIterator();
    }

    /**
     * Loads metadata including the tableSpec from the file before reading starts.
     *
     * @param reader the ORC reader
     * @throws Exception thrown if types cannot be detected
     */
    public void loadMetaInfoBeforeRead(final Reader reader) throws Exception {
        TypeDescription orcSchema = reader.getSchema();
        ArrayList<OrcType<?>> orcTypes = new ArrayList<>();

        if (!tyrCreateFromAdditonalMetadata(orcSchema, reader, orcTypes)) {
            // No KNIME metadata in file. Create from ORC schema.
            orcTypes = createSpecFromOrcSchema(orcSchema);
        }

        m_columnReaders = orcTypes.toArray(new OrcType[orcTypes.size()]);
    }

    private boolean tyrCreateFromAdditonalMetadata(final TypeDescription orcSchema, final Reader reader,
        final ArrayList<OrcType<?>> orcTypes) throws Exception {
        ArrayList<String> columnNames = new ArrayList<>();
        ArrayList<DataType> columnTypes = new ArrayList<>();
        List<String> keys = reader.getMetadataKeys();
        String name = m_file.getName();

        for (String key : keys) {
            if (key.startsWith("knime_tablename.")) {
                name = key.substring(key.indexOf('.'));
            } else if (key.startsWith("knime.")) {
                columnNames.add(key.substring(key.indexOf('.')));

                ByteBuffer columntype = reader.getMetadataValue(key);
                String typeClassString = String.valueOf(columntype.asCharBuffer());
                @SuppressWarnings("unchecked")
                DataType type = DataType.getType((Class<? extends DataCell>)Class.forName(typeClassString));
                columnTypes.add(type);
                orcTypes.add(OrcTableStoreFormat.createOrcType(type));
            }
        }
        if (columnNames.size() == orcSchema.getFieldNames().size()) {
            m_tableSpec =
                new DataTableSpec(name, columnNames.toArray(new String[0]), columnTypes.toArray(new DataType[0]));
            return true;
        }
        return false;
    }

    private ArrayList<OrcType<?>> createSpecFromOrcSchema(final TypeDescription orcSchema) throws Exception {
        ArrayList<OrcType<?>> orcTypes;
        ArrayList<DataType> colTypes = new ArrayList<>();
        orcTypes = new ArrayList<>();

        for (TypeDescription type : orcSchema.getChildren()) {
            Category category = type.getCategory();
            DataType dataType;
            switch (category) {
                case LIST:
                    DataType subtype =
                        m_types.getOrDefault(type.getChildren().get(0).getCategory(), BinaryObjectDataCell.TYPE);
                    dataType = DataType.getType(ListCell.class, subtype);
                    break;
                case MAP:
                    throw new OrcReadException("MAP is not supported yet");
                case UNION:
                    throw new OrcReadException("UNION is not supported yet");
                case STRUCT:
                    throw new OrcReadException("STRUCT is not supported yet");
                default:
                    dataType = m_types.getOrDefault(category, BinaryObjectDataCell.TYPE);
            }
            colTypes.add(dataType);
            orcTypes.add(OrcTableStoreFormat.createOrcType(dataType));
        }

        m_tableSpec = new DataTableSpec(m_file.getName(), orcSchema.getFieldNames().toArray(new String[0]),
            colTypes.toArray(new DataType[0]));
        return orcTypes;
    }

    class DirIterator extends RowIterator {

        /**
         * Creates an iterator that iterates over all files in a directory
         *
         * @throws IOException if files can not be read
         *
         */
        public DirIterator() throws IOException {
            super();
            Reader reader = m_readers.poll();
            m_currentIterator = new OrcRowIterator(reader, m_isReadRowKey, m_batchSize, m_columnReaders, 0);
        }

        OrcRowIterator m_currentIterator;

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            if (!m_currentIterator.hasNext()) {
                try {
                    m_currentIterator.performClose();
                } catch (IOException ex) {
                    LOGGER.info("Closing iterator failed", ex);
                }
                Reader reader = m_readers.poll();
                if (reader == null) {
                    return false;
                } else {
                    try {
                        long index = m_currentIterator.getIndex();
                        m_currentIterator =
                            new OrcRowIterator(reader, m_isReadRowKey, m_batchSize, m_columnReaders, index);
                    } catch (IOException ex) {
                        throw new OrcReadException(ex);
                    }
                    return m_currentIterator.hasNext();
                }
            }
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataRow next() {
            return m_currentIterator.next();
        }
    }

    class OrcRowIterator extends TableStoreCloseableRowIterator {

        private VectorizedRowBatch m_rowBatch;

        private final boolean m_hasRowKey;

        private RecordReader m_rows;

        private int m_rowInBatch;

        private boolean m_isClosed;

        private OrcType<?>[] m_orcTypeReaders;

        private Reader m_orcReader;

        private long m_index;

        /**
         * Row iterator for DataRows read from a ORC file.
         *
         * @param reader the ORC Reader
         * @param batchSize the batch size for reading
         * @throws IOException if file can not be read
         */
        OrcRowIterator(final Reader reader, final boolean hasRowKey, final int batchSize,
            final OrcType<?>[] columnReaders, final long index) throws IOException {
            m_hasRowKey = hasRowKey;
            m_orcReader = reader;
            m_rows = m_orcReader.rows();
            m_rowBatch = m_orcReader.getSchema().createRowBatch(m_batchSize);
            m_orcTypeReaders = columnReaders.clone();
            m_index = index;
            internalNext();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            return m_rowInBatch >= 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            // this is fragile as updates to the batch make it invalid
            OrcRow orcRow = new OrcRow(this, m_rowInBatch, m_index);
            try {
                m_exec.checkCanceled();
            } catch (CanceledExecutionException ex) {
                throw new OrcReadException(ex);
            }
            m_index++;
            DataRow safeRow = new BlobSupportDataRow(orcRow.getKey(), orcRow);
            m_rowInBatch += 1;
            if (m_rowInBatch >= m_rowBatch.size) {
                try {
                    internalNext();
                } catch (IOException e) {
                    throw new OrcReadException(e);
                }
            }
            return safeRow;
        }

        private void internalNext() throws IOException {
            m_exec.setProgress(String.format("Read %d rows", m_index));
            if (m_rows.nextBatch(m_rowBatch)) {
                m_rowInBatch = 0;
            } else {
                m_rowInBatch = -1;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean performClose() throws IOException {
            if (m_isClosed) {
                return false;
            }
            m_rows.close();
            m_rowInBatch = -1;
            m_isClosed = true;
            return true;
        }

        public long getIndex() {
            return m_index;
        }
    }

    /**
     * The KNIME DataRow wrapping the VectorizedBatch, values are read lazy so a reset to the batch will make this row
     * invalid -- caller needs to cache data first.
     */
    static final class OrcRow implements DataRow {

        private final OrcRowIterator m_iterator;

        private final int m_rowInBatch;

        private long m_index;

        /**
         * Creates an ORC Row.
         * @param iterator the row iterator
         * @param rowInBatch the number of the row in the batch
         * @param index the index of the row in the resulting data table
         */
        OrcRow(final OrcRowIterator iterator, final int rowInBatch, final long index) {
            m_iterator = iterator;
            m_rowInBatch = rowInBatch;
            m_index = index;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RowKey getKey() {
            if (m_iterator.m_hasRowKey) {
                String str = ((OrcStringType)m_iterator.m_orcTypeReaders[0])
                    .readString((BytesColumnVector)m_iterator.m_rowBatch.cols[0], m_rowInBatch);
                return new RowKey(str);
            } else {
                return RowKey.createRowKey(m_index);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataCell getCell(final int index) {
            int c = index + (m_iterator.m_hasRowKey ? 1 : 0);
            @SuppressWarnings("unchecked")
            OrcType<ColumnVector> orcType = (OrcType<ColumnVector>)m_iterator.m_orcTypeReaders[c];
            return orcType.readValue(m_iterator.m_rowBatch.cols[c], m_rowInBatch);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getNumCells() {
            return m_iterator.m_orcTypeReaders.length - (m_iterator.m_hasRowKey ? 1 : 0);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Iterator<DataCell> iterator() {
            return new DefaultCellIterator(this);
        }
    }

    /**
     * Get the table spec that was generated from the files informations.
     *
     * @return the table spec read from the file.
     */
    public DataTableSpec getTableSpec() {
        return m_tableSpec;
    }

}
