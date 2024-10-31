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

import com.teragrep.pth10.steps.teragrep.bloomfilter.factory.BloomFilterTableNameFactory;
import com.teragrep.pth10.steps.teragrep.bloomfilter.factory.JournalDBNameFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public final class TableSQL {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableSQL.class);
    private final String name;
    private final String journalDBName;
    private final boolean ignoreConstraints;

    // used in testing
    public TableSQL(String name) {
        this(name, "journaldb");
    }

    // used in testing
    public TableSQL(String name, boolean ignoreConstraints) {
        this(name, "journaldb", ignoreConstraints);
    }

    public TableSQL(BloomFilterTableNameFactory tableNameFactory, JournalDBNameFactory journalDBNameFactory) {
        this(tableNameFactory.configured(), journalDBNameFactory.configured(), false);
    }

    public TableSQL(
            BloomFilterTableNameFactory tableNameFactory,
            JournalDBNameFactory journalDBNameFactory,
            boolean ignoreConstraints
    ) {
        this(tableNameFactory.configured(), journalDBNameFactory.configured(), ignoreConstraints);
    }

    public TableSQL(String name, String journalDBName) {
        this(name, journalDBName, false);
    }

    public TableSQL(String name, String journalDBName, boolean ignoreConstraints) {
        this.name = name;
        this.journalDBName = journalDBName;
        this.ignoreConstraints = ignoreConstraints;
    }

    public String createTableSQL() {
        final String sql;
        final String validName = new ValidTableName(name).name();
        if (ignoreConstraints) {
            sql = "CREATE TABLE IF NOT EXISTS `" + validName + "`("
                    + "`id` BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,"
                    + "`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE," + "`filter_type_id` BIGINT UNSIGNED NOT NULL,"
                    + "`filter` LONGBLOB NOT NULL);";
        }
        else {
            final String validJournalDBName = new ValidTableName(journalDBName).name();
            sql = "CREATE TABLE IF NOT EXISTS `" + validName + "`("
                    + "`id` BIGINT UNSIGNED NOT NULL auto_increment PRIMARY KEY,"
                    + "`partition_id` BIGINT UNSIGNED NOT NULL UNIQUE," + "`filter_type_id` BIGINT UNSIGNED NOT NULL,"
                    + "`filter` LONGBLOB NOT NULL," + "CONSTRAINT `" + validName
                    + "_ibfk_1` FOREIGN KEY (filter_type_id) REFERENCES filtertype (id)" + "ON DELETE CASCADE,"
                    + "CONSTRAINT `" + validName + "_ibfk_2` FOREIGN KEY (partition_id) REFERENCES "
                    + validJournalDBName + ".logfile (id)" + "ON DELETE CASCADE" + ");";
        }
        return sql;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null)
            return false;
        if (object.getClass() != this.getClass())
            return false;
        final TableSQL cast = (TableSQL) object;
        return this.name.equals(cast.name) && this.ignoreConstraints == cast.ignoreConstraints;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, journalDBName, ignoreConstraints);
    }
}
