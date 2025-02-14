CREATE DATABASE IF NOT EXISTS streamdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS journaldb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS bloomdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

GRANT ALL PRIVILEGES ON streamdb.* TO 'test'@'%' IDENTIFIED BY 'test_pass';
GRANT ALL PRIVILEGES ON journaldb.* TO 'test'@'%' IDENTIFIED BY 'test_pass';
GRANT ALL PRIVILEGES ON bloomdb.* TO 'test'@'%' IDENTIFIED BY 'test_pass';

FLUSH PRIVILEGES;

