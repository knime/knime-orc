/*
 * ------------------------------------------------------------------------
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
 * -------------------------------------------------------------------
 *
 * History
 *   Mar 18, 2016 (wiswedel): created
 */
package org.knime.orc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.BlobSupportDataRow;
import org.knime.core.data.container.storage.AbstractTableStoreReader;
import org.knime.core.data.def.DefaultCellIterator;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.orc.types.AbstractOrcType;
import org.knime.orc.types.OrcStringTypeFactory.OrcStringType;
import org.knime.orc.types.OrcType;

/**
 * Implementation of {@link AbstractTableStoreReader} for ORC
 *
 * @author Bernd Wiswedel, KNIME AG, Zuerich, Switzerland
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
// TODO make interface for later
public final class OrcTableStoreReader extends AbstractTableStoreReader {

    private File m_file;

    private boolean m_isReadRowKey;

    private int m_batchSize;

    private OrcType<?>[] m_columnReaders;

    public OrcTableStoreReader(final File file, final boolean isReadRowKey, final NodeSettingsRO settings,
        final int version) throws IOException, InvalidSettingsException {
        super(file, settings, version);
        m_file = file;
        m_isReadRowKey = isReadRowKey;
        // TODO make this configurable from outside?
        m_batchSize = VectorizedRowBatch.DEFAULT_SIZE;
    }

    /** {@inheritDoc} */
    @Override
    public OrcRowIterator iterator() throws IOException {
        final Reader reader =
            OrcFile.createReader(new Path(m_file.getAbsolutePath()), OrcFile.readerOptions(new Configuration()));

        if (m_columnReaders == null) {
            // TODO beautify exception message and add logging
            throw new IOException(
                "No information for type deserialization loaded. Most likely an implementation error.");
        }
        return new OrcRowIterator(reader, m_isReadRowKey, m_batchSize, m_columnReaders);
    }

    /**
     * @param settings contain {@link AbstractOrcType}s used to write columns.
     * @throws InvalidSettingsException thrown in case something goes wrong during de-serialization, e.g. a new version
     *             of a writer has been used which hasn't been installed on the current system.
     */
    @Override
    protected void readMetaFromFile(final NodeSettingsRO settings, final File fileStoreDir, final int version)
            throws IOException, InvalidSettingsException {
        try {
            final List<OrcType<?>> types = new ArrayList<>();
            NodeSettingsRO columnSettings = settings.getNodeSettings("columns");
            for (String typeName : columnSettings) {
                types.add((OrcType<?>)Class.forName((columnSettings.getNodeSettings(typeName).getString("type")))
                    .newInstance());
            }
            m_columnReaders = types.toArray(new OrcType[types.size()]);
        } catch (ClassNotFoundException ex1) {
            // Forward compatibility.
            throw new InvalidSettingsException("Couldn't load class. Missing newer ORC TableStore reader?", ex1);
        } catch (InstantiationException | IllegalAccessException | InvalidSettingsException ex2) {
            throw new InvalidSettingsException("Problems during loading settings for ORC TableStore Reader.", ex2);
        }
    }

    class OrcRowIterator extends TableStoreCloseableRowIterator {

        private final VectorizedRowBatch m_rowBatch;

        private final boolean m_hasRowKey;

        private final RecordReader m_rows;

        private int m_rowInBatch;

        private boolean m_isClosed;

        private OrcType<?>[] m_readers;

        /**
         * @param reader
         * @param readerOptions TODO
         * @param columnTypes TODO
         * @param batchSize TODO
         * @throws IOException
         */
        OrcRowIterator(final Reader reader, final boolean hasRowKey, final int batchSize,
            final OrcType<?>[] columnReaders) throws IOException {
            m_hasRowKey = hasRowKey;
            m_rows = reader.rows();
            m_rowBatch = reader.getSchema().createRowBatch(batchSize);
            m_readers = columnReaders;
            internalNext();
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            return m_rowInBatch >= 0;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataRow next() {
            OrcRow orcRow = new OrcRow(this, m_rowInBatch); // this is fragile as updates to the batch make it invalid
            DataRow safeRow = new BlobSupportDataRow(orcRow.getKey(), orcRow);
            m_rowInBatch += 1;
            if (m_rowInBatch >= m_rowBatch.size) {
                try {
                    internalNext();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return safeRow;
        }

        private void internalNext() throws IOException {
            if (m_rows.nextBatch(m_rowBatch)) {
                m_rowInBatch = 0;
            } else {
                m_rowInBatch = -1;
            }
        }

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
    }

    /**
     * The KNIME DataRow wrapping the VectorizedBatch, values are read lazy so a reset to the batch will make this row
     * invalid -- caller needs to cache data first.
     */
    static final class OrcRow implements DataRow {

        private static final RowKey NO_KEY = new RowKey("no-key");

        private final OrcRowIterator m_iterator;

        private final int m_rowInBatch;

        /**
         * @param iterator
         * @param rowInBatch
         */
        OrcRow(final OrcRowIterator iterator, final int rowInBatch) {
            m_iterator = iterator;
            m_rowInBatch = rowInBatch;
        }

        @Override
        public RowKey getKey() {
            if (m_iterator.m_hasRowKey) {
                String str = ((OrcStringType)m_iterator.m_readers[0])
                    .readString((BytesColumnVector)m_iterator.m_rowBatch.cols[0], m_rowInBatch);
                return new RowKey(str);
            } else {
                return NO_KEY;
            }
        }

        @Override
        public DataCell getCell(final int index) {
            int c = index + (m_iterator.m_hasRowKey ? 1 : 0);
            @SuppressWarnings("unchecked")
            OrcType<ColumnVector> orcType = (OrcType<ColumnVector>)m_iterator.m_readers[c];
            return orcType.readValue(m_iterator.m_rowBatch.cols[c], m_rowInBatch);
        }

        @Override
        public int getNumCells() {
            return m_iterator.m_readers.length - (m_iterator.m_hasRowKey ? 1 : 0);
        }

        @Override
        public Iterator<DataCell> iterator() {
            return new DefaultCellIterator(this);
        }
    }
}
