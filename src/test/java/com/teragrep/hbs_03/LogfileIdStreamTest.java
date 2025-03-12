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
package com.teragrep.hbs_03;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

public class LogfileIdStreamTest {

    @Test
    public void testRange() {
        final long maxBatchSize = 1000;
        final LogfileIdStream logfileIdStream = new LogfileIdStream(1, 10000, maxBatchSize);
        int loops = 0;
        while (logfileIdStream.hasNext()) {
            Block block = logfileIdStream.next();
            long start = block.start();
            long end = block.end();
            Assertions.assertTrue(start < end);
            long difference = end - start;
            Assertions.assertTrue(difference <= 1000);
            loops++;
        }
        Assertions.assertEquals(10, loops);
    }

    @Test
    public void testInvalidRange() {
        final long maxBatchSize = 1000;
        final LogfileIdStream logfileIdStream1 = new LogfileIdStream(10, 10, maxBatchSize);
        Assertions.assertFalse(logfileIdStream1.hasNext());

        final LogfileIdStream logfileIdStream2 = new LogfileIdStream(11, 10, maxBatchSize);
        Assertions.assertFalse(logfileIdStream2.hasNext());

        final LogfileIdStream logfileIdStream3 = new LogfileIdStream(0, -10, maxBatchSize);
        Assertions.assertFalse(logfileIdStream3.hasNext());
    }

    @Test
    public void testBlockNotOverMaxId() {
        final long maxBatchSize = 1000;
        final LogfileIdStream logfileIdStream = new LogfileIdStream(1, 1500, maxBatchSize);

        int loops = 0;
        Block lastBlock = null;
        while (logfileIdStream.hasNext()) {
            lastBlock = logfileIdStream.next();
            final long start = lastBlock.start();
            final long end = lastBlock.end();
            Assertions.assertTrue(start < end);
            Assertions.assertTrue((end - start) <= maxBatchSize);
            loops++;
        }
        Assertions.assertNotNull(lastBlock);
        Assertions.assertFalse(lastBlock.isStub());
        Assertions.assertEquals(1500, lastBlock.end());
        Assertions.assertEquals(1001, lastBlock.start());
        Assertions.assertEquals(2, loops);
    }

    @Test
    public void testCallingNextWhenHasNextFalse() {
        final long maxBatchSize = 1000;
        final LogfileIdStream logfileIdStream = new LogfileIdStream(10, 10, maxBatchSize);
        Assertions.assertFalse(logfileIdStream.hasNext());
        final NoSuchElementException noSuchElementException = Assertions
                .assertThrows(NoSuchElementException.class, logfileIdStream::next);
        final String expectedMessage = "No next block available";
        Assertions.assertEquals(expectedMessage, noSuchElementException.getMessage());
    }
}
