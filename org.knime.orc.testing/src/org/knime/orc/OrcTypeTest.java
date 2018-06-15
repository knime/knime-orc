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
 *   Jan 5, 2018 (wiswedel): created
 */
package org.knime.orc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsSame.sameInstance;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.hamcrest.number.OrderingComparison;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.Buffer;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.node.NodeSettings;
import org.knime.orc.OrcTableStoreReader.OrcRowIterator;

/**
 *
 * @author wiswedel
 */
@RunWith(Parameterized.class)
public final class OrcTypeTest {

    private static final RowKey CONSTANT_KEY = new RowKey("test-no-key");

    @Rule
    public TemporaryFolder m_tempFolder = new TemporaryFolder();

    /** Test is parameterized for different types, see {@link OrcKNIMEType}. */
    @Parameters(name = "{index}: ORCKNIMEType: {0}")
    public static OrcKNIMETestType[] getTestTypes() {
        return new OrcKNIMETestType[]{OrcKNIMETestType.TEST_DOUBLE, OrcKNIMETestType.TEST_INT,
            OrcKNIMETestType.TEST_LONG, OrcKNIMETestType.TEST_STRING,};
    }

    @Parameter
    public OrcKNIMETestType m_orcKNIMETestType;

    @Test
    public void writeSimpleCellNoHeader() throws Exception {
        File tempFile = new File(m_tempFolder.getRoot(), "file.orc"); // must not exist
        DataTableSpec spec =
            new DataTableSpec(new DataColumnSpecCreator("test", m_orcKNIMETestType.getKNIMEColumnType()).createSpec());

        OrcTableStoreWriter writer = new OrcTableStoreWriter(tempFile, spec, false);
        DataCell singleCell = m_orcKNIMETestType.createTestCell(0);
        DataRow row = new DefaultRow(CONSTANT_KEY, singleCell);
        writer.writeRow(row);
        writer.close();
        Assert.assertTrue("File not created " + tempFile.getAbsolutePath(), tempFile.isFile());
        Assert.assertThat("File length unexpected " + tempFile.getAbsolutePath(), FileUtils.sizeOf(tempFile),
            OrderingComparison.greaterThan(0L));
        NodeSettings settings = new NodeSettings("temp");
        writer.writeMetaInfoAfterWrite(settings);

        OrcTableStoreReader reader = new OrcTableStoreReader(tempFile, false, settings, Buffer.IVERSION);
        reader.readMetaFromFile(settings, Buffer.IVERSION);

        OrcRowIterator rowIterator = reader.iterator();
        Assert.assertThat("Iterator has rows", rowIterator.hasNext(), is(true));
        DataRow dataRow = rowIterator.next();
        Assert.assertThat("Row length", dataRow.getNumCells(), is(1));
        Assert.assertThat("Single cell in first (and last)", dataRow.getCell(0), not(sameInstance(singleCell)));
        Assert.assertThat("Single cell in first (and last)", dataRow.getCell(0), equalTo(singleCell));
        Assert.assertThat("Iterator with more than one row", rowIterator.hasNext(), is(false));
    }

    @Test
    public void writeManyNoMissingsWithHeader() throws Exception {
        File tempFile = new File(m_tempFolder.getRoot(), "file.orc");
        writeTestImplementation(tempFile, rowIndex -> m_orcKNIMETestType.createTestCell(rowIndex), true);
    }

    @Test
    public void writeManyWithMissingsNoHeader() throws Exception {
        File tempFile = new File(m_tempFolder.getRoot(), "file.orc");
        Function<Long, DataCell> valueFunction = rowIndex -> {
            if ((rowIndex + 1) % 200 == 0) {
                return DataType.getMissingCell();
            }
            return m_orcKNIMETestType.createTestCell(rowIndex);
        };
        writeTestImplementation(tempFile, valueFunction, false);
    }

    @Test
    public void writeAllConstantNoHeader() throws Exception {
        final DataCell constantCell = m_orcKNIMETestType.createTestCell(17);
        File tempFile = new File(m_tempFolder.getRoot(), "file.orc");
        writeTestImplementation(tempFile, rowIndex -> constantCell, false);
        // file size must be really small despite number of rows
        Assert.assertThat("File length unexpected " + tempFile.getAbsolutePath(), FileUtils.sizeOf(tempFile),
            OrderingComparison.lessThan(20 * 1024L));
    }

    @Test
    public void writeAllMissingsNoHeader() throws Exception {
        File tempFile = new File(m_tempFolder.getRoot(), "file.orc");
        writeTestImplementation(tempFile, rowIndex -> DataType.getMissingCell(), false);
        // file size must be really small despite number of rows
        Assert.assertThat("File length unexpected " + tempFile.getAbsolutePath(), FileUtils.sizeOf(tempFile),
            OrderingComparison.lessThan(20 * 1024L));
    }

    public void writeTestImplementation(final File tempFile, final Function<Long, DataCell> valueFunction,
        final boolean testHeader) throws Exception {
        OrcTableStoreWriter orcWriter = prepOrcWriter(tempFile, testHeader);
        final int rowCount = 5000;
        for (long i = 0; i < rowCount; i++) {
            DataCell singleCell = valueFunction.apply(i);
            DataRow row = new DefaultRow(RowKey.createRowKey(i), singleCell);
            orcWriter.writeRow(row);
        }
        orcWriter.close();
        NodeSettings settings = new NodeSettings("temp");
        orcWriter.writeMetaInfoAfterWrite(settings);
        Assert.assertTrue("File not created " + tempFile.getAbsolutePath(), tempFile.isFile());
        Assert.assertThat("File length unexpected " + tempFile.getAbsolutePath(), FileUtils.sizeOf(tempFile),
            OrderingComparison.greaterThan(0L));

        OrcTableStoreReader reader = new OrcTableStoreReader(tempFile, testHeader, settings, Buffer.IVERSION);
        reader.readMetaFromFile(settings, Buffer.IVERSION);

        OrcRowIterator rowIterator = reader.iterator();
        for (long i = 0; i < rowCount; i++) {
            Assert.assertThat("Iterator has row with index " + i, rowIterator.hasNext(), is(true));
            DataRow dataRow = rowIterator.next();
            Assert.assertThat("Row length", dataRow.getNumCells(), is(1));
            DataCell expectedCell = valueFunction.apply(i);
            DataCell actualCell = dataRow.getCell(0);
            Assert.assertThat("Cell in row " + i, actualCell, equalTo(expectedCell));

            if (testHeader) {
                RowKey actualKey = dataRow.getKey();
                RowKey expectedKey = RowKey.createRowKey(i);
                Assert.assertThat("Key in row " + i, actualKey, equalTo(expectedKey));
            }
        }
        Assert.assertThat("Iterator has row with index " + rowCount, rowIterator.hasNext(), is(false));
    }

    /**
     * Create a builder, make some configurations to the builder so that it handles our small test data like if it was
     * 'big'.
     *
     * @throws IOException
     * @throws IllegalArgumentException
     */
    private OrcTableStoreWriter prepOrcWriter(final File file, final boolean hasKey)
        throws IllegalArgumentException, IOException {
        // must not exist
        DataTableSpec spec = new DataTableSpec(new DataColumnSpecCreator(m_orcKNIMETestType.getClass().getSimpleName(),
            m_orcKNIMETestType.getKNIMEColumnType()).createSpec());
        // batchSize  to force some further processing
        // stripeSize default is (was?) 64MB -- now it's 64kB to force multiple stripes
        OrcTableStoreWriter builder = new OrcTableStoreWriter(file, spec, hasKey, 256, 64 * 1024L);
        return builder;
    }

}
