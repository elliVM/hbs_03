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
package com.teragrep.hbs_03.hbase.config;

import com.teragrep.hbs_03.HbsRuntimeException;
import com.teragrep.hbs_03.Source;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class HBaseConfigWithResource implements Source<Configuration> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseConfigWithResource.class);

    private final Source<Configuration> origin;
    private final Path path;

    public HBaseConfigWithResource(final Source<Configuration> origin, final String pathString) {
        this(origin, Paths.get(pathString));
    }

    public HBaseConfigWithResource(final Source<Configuration> origin, final Path path) {
        this.origin = origin;
        this.path = path;
    }

    public Configuration value() {
        final Configuration config = origin.value();
        if (!Files.exists(path)) {
            throw new HbsRuntimeException(
                    "Could not find a file in given file path",
                    new MalformedURLException("No file in path")
            );
        }
        else {
            try {
                // checks the file system, not the class path
                config.addResource(path.toAbsolutePath().toUri().toURL());
                LOGGER.info("Loaded HBase configurations from from file in path=<[{}]>", path);
            }
            catch (final MalformedURLException e) {
                throw new HbsRuntimeException("Error getting options file", e);
            }
        }
        return config;
    }
}
