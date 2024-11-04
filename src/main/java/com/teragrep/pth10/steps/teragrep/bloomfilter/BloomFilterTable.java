/*
 * Teragrep Data Processing Language (DPL) translator for Apache Spark (pth_10)
 * Copyright (C) 2019-2024 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
 */
package com.teragrep.pth10.steps.teragrep.bloomfilter;

import com.teragrep.pth10.steps.teragrep.bloomfilter.factory.ConnectionFactory;
import com.teragrep.pth10.steps.teragrep.bloomfilter.factory.TableSQLFactory;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

public final class BloomFilterTable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BloomFilterTable.class);
    private final String createTableSQL;
    private final Connection connection;

    public BloomFilterTable(Config config) {
        this(new TableSQLFactory(config).configured(), new ConnectionFactory(config).configured());
    }

    public BloomFilterTable(Config config, boolean ignoreConstraints) {
        this(new TableSQLFactory(config, ignoreConstraints).configured(), new ConnectionFactory(config).configured());
    }

    public BloomFilterTable(String createTableSQL, Connection connection) {
        this.createTableSQL = createTableSQL;
        this.connection = connection;
    }

    public void create() {
        try (final PreparedStatement stmt = connection.prepareStatement(createTableSQL)) {
            stmt.execute();
            connection.commit();
            LOGGER.debug("Create table SQL <{}>", createTableSQL);
        }
        catch (SQLException e) {
            throw new RuntimeException("Error creating bloom filter table: " + e);
        }
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null || getClass() != object.getClass())
            return false;
        final BloomFilterTable cast = (BloomFilterTable) object;
        return Objects.equals(createTableSQL, cast.createTableSQL) && Objects.equals(connection, cast.connection);
    }

    @Override
    public int hashCode() {
        return Objects.hash(createTableSQL, connection);
    }
}
