package net.osmand.server.assist;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import javax.annotation.Nullable;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.type.SerializationException;
import org.hibernate.usertype.UserType;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JsonbType implements UserType {

    @Override
    public int[] sqlTypes() {
        return new int[] { Types.JAVA_OBJECT };
    }

    @Override
    public Class<JsonObject> returnedClass() {
        return JsonObject.class;
    }

    @Override
    public boolean equals(final Object x, final Object y) {
        if (x == y) {
            return true;
        }
        if (x == null || y == null) {
            return false;
        }
        return x.equals(y);
    }

    @Override
    public int hashCode(final Object x) {
        if (x == null) {
            return 0;
        }

        return x.hashCode();
    }

    @Nullable
    @Override
    public Object nullSafeGet(final ResultSet rs,
                              final String[] names,
                              final SharedSessionContractImplementor session,
                              final Object owner) throws SQLException {
        final String json = rs.getString(names[0]);
        if (json == null) {
            return new JsonObject();
        }
        final JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(json).getAsJsonObject();
    }
    
    @Override
    public void nullSafeSet(final PreparedStatement st,
                            final Object value,
                            final int index,
                            final SharedSessionContractImplementor session) throws SQLException {
        if (value == null) {
            st.setNull(index, Types.OTHER);
            return;
        }

        st.setObject(index, value.toString(), Types.OTHER);
    }

    @Nullable
    @Override
    public Object deepCopy(@Nullable final Object value) {
        if (value == null) {
            return null;
        }
        final JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(value.toString()).getAsJsonObject();
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public Serializable disassemble(final Object value) {
        final Object deepCopy = deepCopy(value);

        if (!(deepCopy instanceof Serializable)) {
            throw new SerializationException(
                    String.format("deepCopy of %s is not serializable", value), null);
        }

        return (Serializable) deepCopy;
    }

    @Nullable
    @Override
    public Object assemble(final Serializable cached, final Object owner) {
        return deepCopy(cached);
    }

    @Nullable
    @Override
    public Object replace(final Object original, final Object target, final Object owner) {
        return deepCopy(original);
    }

	
}