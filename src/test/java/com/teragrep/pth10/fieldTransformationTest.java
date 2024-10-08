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
package com.teragrep.pth10;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class fieldTransformationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(fieldTransformationTest.class);

    // Use this file for  dataset initialization
    String testFile = "src/test/resources/xmlWalkerTestDataStreaming";
    private StreamingTestUtil streamingTestUtil;

    @BeforeAll
    void setEnv() {
        this.streamingTestUtil = new StreamingTestUtil();
        this.streamingTestUtil.setEnv();
    }

    @BeforeEach
    void setUp() {
        this.streamingTestUtil.setUp();
    }

    @AfterEach
    void tearDown() {
        this.streamingTestUtil.tearDown();
    }

    // ----------------------------------------
    // Tests
    // ----------------------------------------

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void parseFieldsTransformCatTest() {
        String q = "index=index_B | fields _time";
        this.streamingTestUtil.performDPLTest(q, this.testFile, ds -> {
            List<String> expectedValues = new ArrayList<>();
            expectedValues.add("2006-06-06T06:06:06.060+03:00");
            expectedValues.add("2007-07-07T07:07:07.070+03:00");
            expectedValues.add("2008-08-08T08:08:08.080+03:00");
            expectedValues.add("2009-09-09T09:09:09.090+03:00");
            expectedValues.add("2010-10-10T10:10:10.100+03:00");

            List<String> dsAsList = ds
                    .collectAsList()
                    .stream()
                    .map(r -> r.getString(0))
                    .sorted()
                    .collect(Collectors.toList());
            Collections.sort(expectedValues);

            Assertions.assertEquals(5, dsAsList.size());
            for (int i = 0; i < expectedValues.size(); i++) {
                Assertions.assertEquals(expectedValues.get(i), dsAsList.get(i));
            }

            Assertions.assertEquals("[_time: string]", ds.toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void parseFieldsTransformCat2Test() {
        String q = "index=index_B | fields _time host";
        this.streamingTestUtil.performDPLTest(q, this.testFile, res -> {
            Assertions.assertEquals(5, res.count());
            Assertions.assertEquals("[_time: string, host: string]", res.toString());
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void parseFieldsTransformCatDropTest() {
        this.streamingTestUtil.performDPLTest("index=index_B | fields - host", this.testFile, res -> {
            // check that we drop only host-column
            String schema = res.schema().toString();
            Assertions.assertEquals(5, res.count());
            Assertions
                    .assertEquals(
                            "StructType(StructField(_raw,StringType,true),StructField(_time,StringType,true),StructField(id,LongType,true),StructField(index,StringType,true),StructField(offset,LongType,true),StructField(partition,StringType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true))",
                            schema
                    );
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void parseFieldsTransformCatDropSeveralTest() {
        this.streamingTestUtil.performDPLTest("index=index_B | fields - host index partition", this.testFile, res -> {
            String schema = res.schema().toString();
            Assertions.assertEquals(5, res.count());
            Assertions
                    .assertEquals(
                            "StructType(StructField(_raw,StringType,true),StructField(_time,StringType,true),StructField(id,LongType,true),StructField(offset,LongType,true),StructField(source,StringType,true),StructField(sourcetype,StringType,true))",
                            schema
                    );
        });
    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void fieldsWithPlusTest() {
        String query = "index = index_B | fields + offset";
        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            Assertions.assertEquals(5, ds.count());
            Assertions.assertEquals("[offset: bigint]", ds.toString()); //check schema is correct
        });

    }

    @Test
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void fieldsWithMultiplePlusTest() {
        String query = "index = index_B | fields + offset, source, host";
        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            Assertions.assertEquals(5, ds.count());
            Assertions.assertEquals("[offset, source, host]", Arrays.toString(ds.columns())); //check schema is correct
        });

    }

    @Test
    @Disabled(value = "wildcard functionality not implemented, pth-10 issue #275")
    @DisabledIfSystemProperty(
            named = "skipSparkTest",
            matches = "true"
    )
    void fieldsWithWildcardTest() {
        String query = "index = index_B | fields - _*"; // remove internal fields
        this.streamingTestUtil.performDPLTest(query, this.testFile, ds -> {
            Assertions.assertEquals(5, ds.count());
            Assertions
                    .assertEquals("[index, sourcetype, source, host, partition, offset]", Arrays.toString(ds.columns())); //check schema is correct
        });

    }
}
