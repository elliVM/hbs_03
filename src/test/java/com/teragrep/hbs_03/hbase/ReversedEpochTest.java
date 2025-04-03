/*
 * Teragrep Metadata Using HBase (hbs_03)
 * Copyright (C) 2024 Suomen Kanuuna Oy
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
package com.teragrep.hbs_03.hbase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public final class ReversedEpochTest {

    @Test
    public void testPositiveOrdering() {
        final long epoch1 = 1000000L;
        final long epoch2 = 2000000L;
        final ReversedEpoch reversedEpoch1 = new ReversedEpoch(epoch1);
        final ReversedEpoch reversedEpoch2 = new ReversedEpoch(epoch2);
        final long reversed1 = reversedEpoch1.value();
        final long reversed2 = reversedEpoch2.value();
        Assertions.assertTrue(reversed1 > reversed2);
    }

    @Test
    public void testNegativeValueOrdering() {
        final long epoch1 = -1000000L;
        final long epoch2 = -2000000L;
        final ReversedEpoch reversedEpoch1 = new ReversedEpoch(epoch1);
        final ReversedEpoch reversedEpoch2 = new ReversedEpoch(epoch2);
        final long reversed1 = reversedEpoch1.value();
        final long reversed2 = reversedEpoch2.value();
        Assertions.assertTrue(reversed1 < reversed2);
    }

    @Test
    public void testEqualValues() {
        final long epoch1 = 1000000L;
        final long epoch2 = 1000000L;
        final ReversedEpoch reversedEpoch1 = new ReversedEpoch(epoch1);
        final ReversedEpoch reversedEpoch2 = new ReversedEpoch(epoch2);
        final long reversed1 = reversedEpoch1.value();
        final long reversed2 = reversedEpoch2.value();
        Assertions.assertEquals(reversed1, reversed2);
    }

    @Test
    public void testReversedZeroEpoch() {
        final long zeroEpoch = 0;
        final long plusOne = zeroEpoch + 1;
        final long minusOne = zeroEpoch - 1;
        final ReversedEpoch reversedZero = new ReversedEpoch(zeroEpoch);
        final ReversedEpoch reversedPlusOne = new ReversedEpoch(plusOne);
        final ReversedEpoch reversedMinusOne = new ReversedEpoch(minusOne);
        Assertions.assertTrue(reversedPlusOne.value() < reversedZero.value());
        Assertions.assertTrue(reversedZero.value() < reversedMinusOne.value());
    }

    @Test
    public void testReversedNegativeEpoch() {
        final long negativeEpoch = -1000000L;
        final long epoch = 1000000L;
        final ReversedEpoch negativeReversed = new ReversedEpoch(negativeEpoch);
        final ReversedEpoch reversedEpoch = new ReversedEpoch(epoch);
        Assertions.assertTrue(reversedEpoch.value() < negativeReversed.value());
    }
}
