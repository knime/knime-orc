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
package org.knime.orc.tableformat;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;

/**
 * Wrapper around {@link OrcKNIMEType} that has additional factory methods to create elements (e.g. DataCells) that
 * are used during unit testing.
 *
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
public abstract class OrcKNIMETestType {

    public static final OrcKNIMETestType TEST_DOUBLE = new DoubleOrcKNIMETestType();
    public static final OrcKNIMETestType TEST_STRING = new StringOrcKNIMETestType();
    public static final OrcKNIMETestType TEST_INT = new IntOrcKNIMETestType();
    public static final OrcKNIMETestType TEST_LONG = new LongOrcKNIMETestType();

    private final OrcKNIMEType<?> m_type;

    /**
     * @param type
     */
    OrcKNIMETestType(final OrcKNIMEType<?> type) {
        m_type = type;
    }

    /** @return the type */
    final OrcKNIMEType<?> getType() {
        return m_type;
    }

    /** A random (but deterministic) cell value seeded by the row index. */
    abstract DataCell createTestCell(final long rowIndex);

    /** The column type used within KNIME. */
    abstract DataType getKNIMEColumnType();

    /** Types representing Double. */
    private static final class DoubleOrcKNIMETestType extends OrcKNIMETestType {
        DoubleOrcKNIMETestType() {
            super(OrcKNIMEType.DOUBLE);
        }
        @Override
        DataCell createTestCell(final long rowIndex) {
            return new DoubleCell(rowIndex * 0.5);
        }
        @Override
        DataType getKNIMEColumnType() {
            return DoubleCell.TYPE;
        }
    }

    /** Types representing String. */
    private static final class StringOrcKNIMETestType extends OrcKNIMETestType {
        StringOrcKNIMETestType() {
            super(OrcKNIMEType.STRING);
        }
        @Override
        DataCell createTestCell(final long rowIndex) {
            return new StringCell("Cell " + rowIndex);
        }
        @Override
        DataType getKNIMEColumnType() {
            return StringCell.TYPE;
        }
    }

    /** Types representing Int. */
    private static final class IntOrcKNIMETestType extends OrcKNIMETestType {
        IntOrcKNIMETestType() {
            super(OrcKNIMEType.INT);
        }
        @Override
        DataCell createTestCell(final long rowIndex) {
            return new IntCell((int)rowIndex);
        }
        @Override
        DataType getKNIMEColumnType() {
            return IntCell.TYPE;
        }
    }

    /** Types representing Long. */
    private static final class LongOrcKNIMETestType extends OrcKNIMETestType {
        LongOrcKNIMETestType() {
            super(OrcKNIMEType.LONG);
        }
        @Override
        DataCell createTestCell(final long rowIndex) {
            return new LongCell(rowIndex);
        }
        @Override
        DataType getKNIMEColumnType() {
            return LongCell.TYPE;
        }
    }

}
