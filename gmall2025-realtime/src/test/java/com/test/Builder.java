package com.test;

import org.apache.doris.flink.cfg.DorisOptions;


// 构造者设计模式
public  class Builder {
        private String fenodes;
        private String benodes;
        private String jdbcUrl;
        private String username;
        private String password;
        private boolean autoRedirect = true;
        private String tableIdentifier;

        /** required, tableIdentifier. */
        public Builder setTableIdentifier(String tableIdentifier) {
            this.tableIdentifier = tableIdentifier;
            return this;
        }

        /** optional, user name. */
        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        /** optional, password. */
        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /** required, Frontend Http Rest url. */
        public Builder setFenodes(String fenodes) {
            this.fenodes = fenodes;
            return this;
        }

        /** optional, Backend Http Port. */
        public Builder setBenodes(String benodes) {
            this.benodes = benodes;
            return this;
        }

        /** not required, fe jdbc url, for lookup query. */
        public Builder setJdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public Builder setAutoRedirect(boolean autoRedirect) {
            this.autoRedirect = autoRedirect;
            return this;
        }

        public DorisOptions build() {
            // multi table load, don't need check
            // checkNotNull(tableIdentifier, "No tableIdentifier supplied.");
            return new DorisOptions(
                    this.fenodes, benodes, username, password, tableIdentifier, jdbcUrl, autoRedirect);
        }
    }