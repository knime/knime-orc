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
 *   Mar 14, 2016 (wiswedel): created
 */
package org.knime.orc;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.container.ContainerTable;
import org.knime.core.data.container.storage.AbstractTableStoreReader;
import org.knime.core.data.container.storage.AbstractTableStoreWriter;
import org.knime.core.data.container.storage.TableStoreFormat;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.orc.types.OrcDoubleTypeFactory;
import org.knime.orc.types.OrcIntTypeFactory;
import org.knime.orc.types.OrcLongTypeFactory;
import org.knime.orc.types.OrcStringTypeFactory;
import org.knime.orc.types.OrcType;
import org.knime.orc.types.OrcTypeFactory;

public final class OrcTableStoreFormat implements TableStoreFormat {

    // TODO we could add an extension point for this later.
    private final static Map<DataType, OrcTypeFactory<?>> TYPE_FACTORIES = new HashMap<>();

    static {
        register(new OrcStringTypeFactory());
        register(new OrcDoubleTypeFactory());
        register(new OrcIntTypeFactory());
        register(new OrcLongTypeFactory());
    }

    public static synchronized <T extends OrcType<?>> void register(final OrcTypeFactory<T> fac) {
        TYPE_FACTORIES.putIfAbsent(fac.type(), fac);
    }

    public final static <T extends OrcType<?>> T createOrcType(final DataType type) {
        @SuppressWarnings("unchecked")
        final T orcType = (T)TYPE_FACTORIES.get(type).create();
        return orcType;
    }

    @Override
    public String getName() {
        return "Column Store (Apache ORC) - experimental";
    }

    @Override
    public String getFilenameSuffix() {
        return ".orc";
    }

    /** {@inheritDoc} */
    @Override
    public boolean accepts(final DataTableSpec spec) {
        return spec.stream().map(c -> c.getType()).allMatch(t -> TYPE_FACTORIES.containsKey(t));
    }

    /** {@inheritDoc} */
    @Override
    public AbstractTableStoreWriter createWriter(final File binFile, final DataTableSpec spec,
        final boolean writeRowKey) throws IOException {
        return new OrcTableStoreWriter(binFile, spec, writeRowKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AbstractTableStoreWriter createWriter(final OutputStream output, final DataTableSpec spec,
        final boolean writeRowKey) throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException("Writing to stream not supported");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AbstractTableStoreReader createReader(final File binFile, final DataTableSpec spec,
        final NodeSettingsRO settings, final Map<Integer, ContainerTable> tblRep, final int version,
        final boolean isReadRowKey) throws IOException, InvalidSettingsException {
        final OrcTableStoreReader reader = new OrcTableStoreReader(binFile, isReadRowKey);
        reader.loadMetaInfoBeforeRead(settings);
        return reader;
    }

}
