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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.container.ContainerTable;
import org.knime.core.data.container.storage.AbstractTableStoreReader;
import org.knime.core.data.container.storage.AbstractTableStoreWriter;
import org.knime.core.data.container.storage.TableStoreFormat;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.orc.types.OrcBinaryTypeFactory;
import org.knime.orc.types.OrcBooleanTypeFactory;
import org.knime.orc.types.OrcDoubleTypeFactory;
import org.knime.orc.types.OrcIntTypeFactory;
import org.knime.orc.types.OrcListTypeFactory;
import org.knime.orc.types.OrcListTypeFactory.OrcListType;
import org.knime.orc.types.OrcLongTypeFactory;
import org.knime.orc.types.OrcStringTypeFactory;
import org.knime.orc.types.OrcTimestampTypeFactory;
import org.knime.orc.types.OrcType;
import org.knime.orc.types.OrcTypeFactory;

/**
 * Table Store Format for ORC.
 *
 * @author
 */
public final class OrcTableStoreFormat implements TableStoreFormat {

    // TODO we could add an extension point for this later.
    private static final Map<DataType, OrcTypeFactory<?>> TYPE_FACTORIES = new HashMap<>();

    static {
        register(new OrcStringTypeFactory());
        register(new OrcDoubleTypeFactory());
        register(new OrcIntTypeFactory());
        register(new OrcLongTypeFactory());
        register(new OrcBinaryTypeFactory());
        register(new OrcBooleanTypeFactory());
        register(new OrcBinaryTypeFactory());
        register(new OrcTimestampTypeFactory());
    }

    /**
     * Registers a OrcTypeFactory.
     *
     * @param fac the factory to register
     */
    public static synchronized <T extends OrcType<?>> void register(final OrcTypeFactory<T> fac) {
        TYPE_FACTORIES.putIfAbsent(fac.type(), fac);
    }

    /**
     * Creates a OrcType for the given {@link DataType}.
     *
     * @param type the data type
     * @return The corresponding OrcType for the type
     */
    @SuppressWarnings("unchecked")
    public static final <T extends OrcType<?>> T createOrcType(final DataType type) {
        if (type.isCollectionType()) {
            OrcListTypeFactory factory = new OrcListTypeFactory();

            OrcTypeFactory<?> orcTypeFactory = TYPE_FACTORIES.get(type.getCollectionElementType());
            if (orcTypeFactory == null) {
                throw new OrcReadException(
                    String.format("List of type %s not supported yet.", type.getCollectionElementType()));
            }
            OrcType<ColumnVector> subtype = (OrcType<ColumnVector>)orcTypeFactory.create();
            final OrcListType orcType = factory.create(subtype);
            return (T)orcType;
        }
        if (TYPE_FACTORIES.get(type) == null) {
            throw new OrcWriteException(String.format("Type %s not supported yet", type.getName()));
        }
        return (T)TYPE_FACTORIES.get(type).create();
    }

    /**
     * Returns an array of all unsupported types for a given table spec
     *
     * @param spec the table spec to check
     * @return the array containing the unsupported types
     */
    public String[] getUnsupportedTypes(final DataTableSpec spec) {
        List<String> unsupported = spec.stream().map(DataColumnSpec::getType).filter(t -> !t.isCollectionType())
            .filter(t -> !TYPE_FACTORIES.containsKey(t)).map(DataType::getName).collect(Collectors.toList());
        spec.stream().map(DataColumnSpec::getType).filter(t -> t.isCollectionType())
        .filter(t -> !TYPE_FACTORIES.containsKey(t.getCollectionElementType())).map(DataType::getName).collect(Collectors.toList());
        return unsupported.toArray(new String[unsupported.size()]);
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
        boolean listsSupported = spec.stream().map(DataColumnSpec::getType).filter(DataType::isCollectionType)
            .allMatch(t -> TYPE_FACTORIES.containsKey(t.getCollectionElementType()));
        boolean otherSupported = spec.stream().map(DataColumnSpec::getType).filter(t -> !t.isCollectionType())
            .allMatch(t -> TYPE_FACTORIES.containsKey(t));
        return listsSupported && otherSupported;
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
        final boolean writeRowKey) throws IOException {
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
