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

import com.teragrep.pth10.steps.teragrep.bloomfilter.factory.FilterFieldsJsonObjectListFactory;
import com.teragrep.pth10.steps.teragrep.bloomfilter.factory.FilterOptionValues;
import com.typesafe.config.Config;
import org.apache.spark.util.sketch.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public final class FilterTypes implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterTypes.class);

    private final List<FilterOptionValues> filterValuesList;

    public FilterTypes(Config config) {
        this(new FilterFieldsJsonObjectListFactory(config).configured());
    }

    public FilterTypes(List<FilterOptionValues> filterValuesList) {
        this.filterValuesList = filterValuesList;
    }

    /**
     * Filter sizes as sorted map
     * <p>
     * Keys = filter expected num of items, values = filter FPP
     *
     * @return SortedMap of filter configuration
     */
    public SortedMap<Long, Double> sortedMap() {
        final SortedMap<Long, Double> sizesMapFromJson = new TreeMap<>();
        for (final FilterOptionValues values : filterValuesList) {
            final Long expectedNumOfItems = values.expected();
            final Double fpp = values.fpp();
            if (sizesMapFromJson.containsKey(expectedNumOfItems)) {
                LOGGER.error("Duplicate value of expected number of items value: <[{}]>", expectedNumOfItems);
                throw new RuntimeException("Duplicate entry expected num of items");
            }
            sizesMapFromJson.put(expectedNumOfItems, fpp);
        }
        return sizesMapFromJson;
    }

    public Map<Long, Long> bitSizeMap() {
        final Map<Long, Double> filterSizes = sortedMap();
        final Map<Long, Long> bitsizeToExpectedItemsMap = new HashMap<>();
        // Calculate bitSizes
        for (final Map.Entry<Long, Double> entry : filterSizes.entrySet()) {
            final BloomFilter bf = BloomFilter.create(entry.getKey(), entry.getValue());
            bitsizeToExpectedItemsMap.put(bf.bitSize(), entry.getKey());
        }
        return bitsizeToExpectedItemsMap;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object)
            return true;
        if (object == null)
            return false;
        if (object.getClass() != this.getClass())
            return false;
        final FilterTypes cast = (FilterTypes) object;
        return filterValuesList.equals(cast.filterValuesList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filterValuesList);
    }
}
