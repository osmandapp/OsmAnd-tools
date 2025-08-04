package net.osmand.server;

import org.hibernate.community.dialect.SQLiteDialect;
import org.hibernate.dialect.identity.IdentityColumnSupport;
import org.hibernate.dialect.identity.IdentityColumnSupportImpl;

public class StrictSQLiteDialect extends SQLiteDialect {

	@Override
	public IdentityColumnSupport getIdentityColumnSupport() {
		return new IdentityColumnSupportImpl() {

			@Override                 // ➊ ask Hibernate to call the SELECT
			public boolean supportsInsertSelectIdentity() {
				return true;
			}

			@Override                 // ➋ the SELECT itself
			public String getIdentitySelectString(String tbl, String col, int type) {
				return "select last_insert_rowid()";
			}

			@Override                 // ➌ column fragment used in DDL
			public String getIdentityColumnString(int type) {
				return "integer";     // (= 64-bit)     add " primary key autoincrement"
			}                         //                 here if you need the keyword

			@Override                 // ➍ we *did* put the type in ➌
			public boolean hasDataTypeInIdentityColumn() {
				return true;
			}
		};
	}
}

