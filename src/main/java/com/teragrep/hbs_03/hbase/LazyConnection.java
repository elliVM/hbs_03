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

import com.teragrep.hbs_03.HbsRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.cactoos.Scalar;
import org.cactoos.scalar.IoChecked;
import org.cactoos.scalar.Sticky;
import org.cactoos.scalar.Synced;

/**
 * Wrapper for {@link Connection} that provides lazy initialization of the underlying HBase {@link Connection}.
 *
 * @see org.apache.hadoop.hbase.client.Connection
 * @see org.apache.hadoop.conf.Configuration
 * @see Scalar
 * @see <a href="https://github.com/yegor256/cactoos">Cactoos Project</a>
 */
public final class LazyConnection implements Scalar<Connection> {

    private final Scalar<Connection> scalarConnection;

    public LazyConnection(final Configuration configuration, final int fixedThreadPoolCount) {
        this(new ScalarConnection(configuration, fixedThreadPoolCount));
    }

    public LazyConnection(final Scalar<Connection> scalarConnection) {
        this.scalarConnection = new IoChecked<>(new Synced<>(new Sticky<>(scalarConnection)));
    }

    /**
     * Lazily initialized HBase {@link Connection}, created on the first time this method is called.
     * <p>
     * Note that the returned connection is thread-safe and can be shared between threads and objects
     * 
     * @see org.apache.hadoop.hbase.client.Connection
     */
    @SuppressWarnings("IllegalCatch")
    public Connection value() {
        final Connection returnValue;
        try {
            returnValue = scalarConnection.value();
        }
        catch (final Exception e) {
            throw new HbsRuntimeException("Exception getting connection", e);
        }
        return returnValue;
    }

}
