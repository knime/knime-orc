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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.WriterOptions;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.storage.AbstractTableStoreWriter;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.util.Pair;
import org.knime.orc.types.OrcStringTypeFactory.OrcStringType;
import org.knime.orc.types.OrcType;

/**
 * TODO describe that this guy has a state etc
 *
 * @author Bernd Wiswedel, KNIME AG, Zuerich, Switzerland
 * @author Christian Dietz, KNIME GmbH, Konstanz, Germany
 */
public class OrcTableStoreWriter extends AbstractTableStoreWriter {

    private final File m_binFile;

    private final OrcWriter m_orcKNIMEWriter;

    private final long m_stripSize;

    private final int m_batchSize;

    private List<Pair<String, OrcType<?>>> m_fields;

    /**
     * @param file
     * @param spec
     * @param isWriteRowKey
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public OrcTableStoreWriter(final File file, final DataTableSpec spec, final boolean isWriteRowKey,
        final int batchSize, final long stripeSize) throws IllegalArgumentException, IOException {
        super(spec, isWriteRowKey);
        m_binFile = file;
        m_binFile.delete();
        m_batchSize = batchSize;
        m_stripSize = stripeSize;
        m_orcKNIMEWriter = initWriter();
    }

    /**
     * @param file
     * @param spec
     * @param isWriteRowKey
     * @throws IOException
     * @throws IllegalArgumentException
     */
    public OrcTableStoreWriter(final File file, final DataTableSpec spec, final boolean isWriteRowKey)
        throws IllegalArgumentException, IOException {
        this(file, spec, isWriteRowKey, VectorizedRowBatch.DEFAULT_SIZE, -1l);
    }

    /**
     * @return
     * @throws IOException
     * @throws IllegalArgumentException
     */
    private OrcWriter initWriter() throws IllegalArgumentException, IOException {
        m_fields = new ArrayList<>();
        if (isWriteRowKey()) {
            // TODO extract key to static variable. somewhere. later.
            m_fields.add(new Pair<>("row-key", OrcTableStoreFormat.createOrcType(StringCell.TYPE)));
        }
        final DataTableSpec spec = getSpec();
        for (int i = 0; i < spec.getNumColumns(); i++) {
            DataColumnSpec colSpec = spec.getColumnSpec(i);
            String name = i + "-" + colSpec.getName().replaceAll("[^a-zA-Z]", "-");
            m_fields.add(new Pair<>(name, OrcTableStoreFormat.createOrcType(colSpec.getType())));

        }
        Configuration conf = new Configuration();
        TypeDescription schema = deriveTypeDescription();
        WriterOptions orcConf = OrcFile.writerOptions(conf).setSchema(schema).compress(CompressionKind.SNAPPY)
            .version(OrcFile.Version.V_0_12);
        if (m_stripSize > -1) {
            orcConf.stripeSize(m_stripSize);
        }

        Writer writer = OrcFile.createWriter(new Path(m_binFile.getAbsolutePath()), orcConf);
        VectorizedRowBatch rowBatch = schema.createRowBatch(m_batchSize);
        return new OrcWriter(writer, isWriteRowKey(), rowBatch, getOrcKNIMETypes());
    }

    private OrcType<?>[] getOrcKNIMETypes() {
        final List<OrcType<?>> collect = m_fields.stream().map((p) -> p.getSecond()).collect(Collectors.toList());
        return collect.toArray(new OrcType<?>[collect.size()]);
    }

    /** Internal helper */
    private TypeDescription deriveTypeDescription() {
        TypeDescription schema = TypeDescription.createStruct();
        for (Pair<String, OrcType<?>> colEntry : m_fields) {
            schema.addField(colEntry.getFirst(), colEntry.getSecond().getTypeDescription());
        }
        return schema;
    }

    /** {@inheritDoc} */
    @Override
    public void writeRow(final DataRow row) throws IOException {
        m_orcKNIMEWriter.addRow(row);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        m_orcKNIMEWriter.close();
    }

    /** {@inheritDoc} */
    @Override
    public void writeMetaInfoAfterWrite(final NodeSettingsWO settings) {
        NodeSettingsWO columnsSettings = settings.addNodeSettings("columns");
        for (Pair<String, OrcType<?>> entry : m_fields) {
            // Remember the type which has been used to write this thingy..
            final NodeSettingsWO colSetting = columnsSettings.addNodeSettings(entry.getFirst());
            colSetting.addString("type", entry.getSecond().getClass().getName());
        }
    }

    class OrcWriter implements AutoCloseable {

        private final Writer m_writer;

        private final OrcType<?>[] m_columnTypes;

        private final VectorizedRowBatch m_rowBatch;

        private final boolean m_hasRowKey;

        OrcWriter(final Writer writer, final boolean hasRowKey, final VectorizedRowBatch rowBatch,
            final OrcType<?>[] columnTypes) {
            m_writer = writer;
            m_hasRowKey = hasRowKey;
            m_columnTypes = columnTypes;
            m_rowBatch = rowBatch;
        }

        public void addRow(final DataRow row) throws IOException {
            final int rowInBatch = m_rowBatch.size++;
            int c = 0;
            if (m_hasRowKey) {
                // TODO this feels wrong. Can we solve this on API level?!
                ((OrcStringType)m_columnTypes[0]).writeString((BytesColumnVector)m_rowBatch.cols[0], rowInBatch,
                    row.getKey().getString());
                c += 1;
            }
            for (; c < m_rowBatch.numCols; c++) {
                DataCell cell = row.getCell(m_hasRowKey ? c - 1 : c);
                @SuppressWarnings("unchecked")
                final OrcType<ColumnVector> type = (OrcType<ColumnVector>)m_columnTypes[c];
                type.writeValue(m_rowBatch.cols[c], rowInBatch, cell);
            }
            if (m_rowBatch.size == m_rowBatch.getMaxSize()) {
                m_writer.addRowBatch(m_rowBatch);
                m_rowBatch.reset();
            }
        }

        @Override
        public void close() throws IOException {
            if (m_rowBatch.size != 0) {
                m_writer.addRowBatch(m_rowBatch);
                m_rowBatch.reset();
            }
            m_writer.close();
        }
    }
}
