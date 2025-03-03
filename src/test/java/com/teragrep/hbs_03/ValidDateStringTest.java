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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Date;

import static org.junit.jupiter.api.Assertions.*;

public class ValidDateStringTest {

    @Test
    public void testValidDate() {
        final Date date = new ValidDateString("2010-01-01").date();
        Assertions.assertEquals(Date.valueOf("2010-01-01"), date);
    }

    @Test
    public void testInvalidFormat() {
        final ValidDateString date = new ValidDateString("2010.30.01");
        final HbsRuntimeException ex = assertThrows(HbsRuntimeException.class, date::date);
        final String expectedMessage = "Invalid date format <2010.30.01> Expected format YYYY-MM-DD (caused by: IllegalArgumentException: Invalid date format)";
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    public void testInvalidDay() {
        final ValidDateString date = new ValidDateString("2010-01-33");
        final HbsRuntimeException ex = assertThrows(HbsRuntimeException.class, date::date);
        final String expectedMessage = "Invalid date format <2010-01-33> Expected format YYYY-MM-DD (caused by: IllegalArgumentException: Invalid date format)";
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    public void testInvalidMonth() {
        final ValidDateString date = new ValidDateString("2010-13-10");
        final HbsRuntimeException ex = assertThrows(HbsRuntimeException.class, date::date);
        final String expectedMessage = "Invalid date format <2010-13-10> Expected format YYYY-MM-DD (caused by: IllegalArgumentException: Invalid date format)";
        Assertions.assertEquals(expectedMessage, ex.getMessage());
    }

    @Test
    public void testContract() {
        EqualsVerifier.forClass(ValidDateString.class).withNonnullFields("pattern", "dateString").verify();
    }
}
