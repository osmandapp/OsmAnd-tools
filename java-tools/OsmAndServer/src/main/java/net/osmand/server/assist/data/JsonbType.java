package net.osmand.server.assist.data;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.type.SqlTypes;
import org.hibernate.usertype.UserType;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JsonbType implements UserType<JsonObject> {

    @Override
    public int getSqlType() {
        return SqlTypes.JSON;
    }

    @Override
    public Class<JsonObject> returnedClass() {
        return JsonObject.class;
    }

    @Override
    public boolean equals(JsonObject jsonObject, JsonObject j1) {
        if (jsonObject == j1) {
            return true;
        }
        if (jsonObject == null || j1 == null) {
            return false;
        }
        return jsonObject.equals(j1);
    }

    @Override
    public int hashCode(JsonObject jsonObject) {
        return jsonObject == null ? 0 : jsonObject.hashCode();
    }

    @Override
    public JsonObject nullSafeGet(ResultSet resultSet, int i, SharedSessionContractImplementor sharedSessionContractImplementor, Object o) throws SQLException {
        String json = resultSet.getString(i);
        if (json == null) {
            return new JsonObject();
        }
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(json).getAsJsonObject();
    }

    @Override
    public void nullSafeSet(PreparedStatement preparedStatement, JsonObject jsonObject, int i, SharedSessionContractImplementor sharedSessionContractImplementor) throws SQLException {
        if (jsonObject == null) {
            preparedStatement.setNull(i, Types.OTHER);
        } else {
            preparedStatement.setObject(i, jsonObject.toString(), Types.OTHER);
        }
    }

    @Override
    public JsonObject deepCopy(JsonObject jsonObject) {
        if (jsonObject == null) {
            return null;
        }
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse(jsonObject.toString()).getAsJsonObject();
    }

    @Override
    public boolean isMutable() {
        return true;
    }

    @Override
    public Serializable disassemble(JsonObject jsonObject) {
        if (jsonObject == null) {
            return null;
        }
        return jsonObject.toString();
    }

    @Override
    public JsonObject assemble(Serializable serializable, Object o) {
        if (serializable == null) {
            return null;
        }
        JsonParser jsonParser = new JsonParser();
        return jsonParser.parse((String) serializable).getAsJsonObject();
    }

    @Override
    public JsonObject replace(JsonObject original, JsonObject target, Object owner) {
        return deepCopy(original);
    }
}