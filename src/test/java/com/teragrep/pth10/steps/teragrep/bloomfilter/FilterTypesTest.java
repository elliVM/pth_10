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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.util.sketch.BloomFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class FilterTypesTest {

    final String username = "sa";
    final String password = "";
    final String connectionUrl = "jdbc:h2:mem:test;MODE=MariaDB;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE";
    final Connection conn = Assertions.assertDoesNotThrow(() -> DriverManager.getConnection(connectionUrl, username, password));

    @BeforeEach
    public void setup() {
        String createFilterType = "CREATE TABLE `filtertype` ("
                + "`id`               bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                + "`expectedElements` bigint(20) NOT NULL," + "`targetFpp`        DOUBLE UNSIGNED NOT NULL,"
                + "`pattern`          VARCHAR(255) NOT NULL)";
        Assertions.assertDoesNotThrow(() -> {
            conn.prepareStatement("DROP TABLE IF EXISTS `filtertype`").execute();
            conn.prepareStatement(createFilterType).execute();
        });
    }
    @Test
    public void testSortedMapMethod() {
        Config config = ConfigFactory.parseProperties(defaultProperties());
        FilterTypes filterTypes = new FilterTypes(config);
        Map<Long, Double> resultMap = filterTypes.sortedMap();
        assertEquals(0.02, resultMap.get(2000L));
        assertEquals(0.01, resultMap.get(1000L));
        assertEquals(0.03, resultMap.get(3000L));
        assertEquals(3, resultMap.size());
        Map<Long,Double> expectedMap = new TreeMap<>();
        expectedMap.put(1000L, 0.01);
        expectedMap.put(2000L, 0.02);
        expectedMap.put(3000L, 0.03);
        Assertions.assertEquals(expectedMap, resultMap);
    }

    @Test
    public void testBitSizeMapMethod() {
        Config config = ConfigFactory.parseProperties(defaultProperties());
        FilterTypes filterTypes = new FilterTypes(config);
        Map<Long, Long> bitSizeMap = filterTypes.bitSizeMap();
        Assertions.assertEquals(1000L, bitSizeMap.get(BloomFilter.create(1000, 0.01).bitSize()));
        Assertions.assertEquals(2000L, bitSizeMap.get(BloomFilter.create(2000, 0.02).bitSize()));
        Assertions.assertEquals(3000L, bitSizeMap.get(BloomFilter.create(3000, 0.03).bitSize()));
        Assertions.assertEquals(3, bitSizeMap.size());

    }

    @Test
    public void testSaveToDatabase() {
        Config config = ConfigFactory.parseProperties(defaultProperties());
        FilterTypes filterTypes = new FilterTypes(config);
        Assertions.assertDoesNotThrow(filterTypes::saveToDatabase);
        ResultSet rs = Assertions.assertDoesNotThrow(() -> conn.prepareStatement("SELECT * FROM `filtertype`").executeQuery());
        Assertions.assertDoesNotThrow(() -> {
            List<Long> expectedItemsList = new ArrayList<>();
            List<Double> fppList = new ArrayList<>();
            int loops = 0;
            while(rs.next()) {
                Long expected = rs.getLong(2);
                Double fpp = rs.getDouble(3);
                expectedItemsList.add(expected);
                fppList.add(fpp);
                Assertions.assertEquals("pattern", rs.getString(4));
                loops++;
            }
            Assertions.assertEquals(3, loops);
            Assertions.assertEquals(Arrays.asList(1000L, 2000L, 3000L), expectedItemsList);
            Assertions.assertEquals(Arrays.asList(0.01, 0.02, 0.03), fppList);
        });
    }

    @Test
    public void testEquals() {
        Config config = ConfigFactory.parseProperties(defaultProperties());
        FilterTypes filterTypes1 = new FilterTypes(config);
        FilterTypes filterTypes2 = new FilterTypes(config);
        filterTypes1.sortedMap();
        filterTypes1.bitSizeMap();
        assertEquals(filterTypes1, filterTypes2);
    }

    @Test
    public void testNotEquals() {
        Properties properties2 = defaultProperties();
        properties2
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "[" + "{expected: 2000, fpp: 0.01}," + "{expected: 2500, fpp: 0.01},"
                                + "{expected: 3000, fpp: 0.01}" + "]"
                );
        Config config1 = ConfigFactory.parseProperties(defaultProperties());
        Config config2 = ConfigFactory.parseProperties(properties2);
        FilterTypes filterTypes1 = new FilterTypes(config1);
        FilterTypes filterTypes2 = new FilterTypes(config2);
        assertNotEquals(filterTypes1, filterTypes2);
    }

    private Properties defaultProperties() {
        Properties properties = new Properties();
        properties.put("dpl.pth_06.bloom.table.name", "filtertype_test");
        properties.put("dpl.pth_10.bloom.db.username", username);
        properties.put("dpl.pth_10.bloom.db.password", password);
        properties.put("dpl.pth_06.bloom.db.url", connectionUrl);
        properties.put("dpl.pth_06.bloom.pattern", "pattern");
        properties
                .put(
                        "dpl.pth_06.bloom.db.fields",
                        "[" + "{expected: 1000, fpp: 0.01}," + "{expected: 2000, fpp: 0.02},"
                                + "{expected: 3000, fpp: 0.03}" + "]"
                );
        return properties;
    }
}
