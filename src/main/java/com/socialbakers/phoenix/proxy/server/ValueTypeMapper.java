package com.socialbakers.phoenix.proxy.server;

import com.google.protobuf.ByteString;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.DataType;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.QueryRequest.Query.Param;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.sql.Date;
import java.util.List;
import org.eclipse.jdt.internal.core.Assert;

public class ValueTypeMapper {
    
    static final int BYTE_SIZE = Byte.SIZE / Byte.SIZE;     // 1B
    static final int SHORT_SIZE = Short.SIZE / Byte.SIZE;   // 2B
    static final int INT_SIZE = Integer.SIZE / Byte.SIZE;   // 4B
    static final int LONG_SIZE = Long.SIZE / Byte.SIZE;     // 8B
    
    
    static ByteString getValue(ResultSet rs, ResultSetMetaData meta,
            int column) throws SQLException {

        Object o = rs.getObject(column);
        if (o == null || rs.wasNull()) {
            return ByteString.EMPTY;
        }

        int type = meta.getColumnType(column);
        
        return ByteString.copyFrom(toBytes(o, type));
    }
    
    static byte[] toBytes(Object o, int type) {
                
        ByteBuffer b = null;
        
        switch (type) {
            case Types.INTEGER:
                b = ByteBuffer.allocate(INT_SIZE).putInt((Integer) o);
                break;
                
            case Types.BIGINT:
                b = ByteBuffer.allocate(LONG_SIZE).putLong((Long) o);
                break;
                
            case Types.TINYINT:
                b = ByteBuffer.allocate(BYTE_SIZE).put((Byte) o);
                break;
                
            case Types.SMALLINT:
                b = ByteBuffer.allocate(SHORT_SIZE).putShort((Short)o);
                break;
                
            case Types.FLOAT:
                b = ByteBuffer.allocate(INT_SIZE).putFloat((Float)o);
                break;
                
            case Types.DOUBLE:
                b = ByteBuffer.allocate(LONG_SIZE).putDouble((Double)o);
                break;
                
            case Types.DECIMAL:
                BigDecimal bd = (BigDecimal)o;
                b = ByteBuffer.allocate(LONG_SIZE).putDouble(bd.doubleValue());
                break;
                
            case Types.BOOLEAN:
                byte by = (byte) (Boolean.TRUE.equals(o) ? 1 : 0);
                b = ByteBuffer.allocate(BYTE_SIZE).put(by);
                break;
                
            case Types.DATE:
                Date d = (Date) o;
                b = ByteBuffer.allocate(LONG_SIZE).putLong(d.getTime());
                break;
                
            case Types.TIME:
                Time t = (Time)o;
                b = ByteBuffer.allocate(LONG_SIZE).putLong(t.getTime());
                break;                
                
            case Types.TIMESTAMP:
                Timestamp ts = (Timestamp)o;
                b = ByteBuffer.allocate(LONG_SIZE + INT_SIZE)
                        .putLong(ts.getTime())
                        .putInt(ts.getNanos());
                break;                
                
            case Types.VARCHAR:
            case Types.CHAR:
                b = ByteBuffer.wrap(ByteString.copyFromUtf8((String) o).toByteArray());
                break;
                
            case Types.BINARY:
            case Types.VARBINARY:
                b = ByteBuffer.wrap((byte[])o);
                break;
                
            default:
                throw new IllegalStateException("Missing mapping for type " + type);
        }
        
        return b.array();
    }
    
    static DataType getColumnType(int type) {

        switch (type) {
            case Types.INTEGER: return DataType.INTEGER; 
            case Types.BIGINT: return DataType.BIGINT;
            case Types.TINYINT: return DataType.TINYINT;
            case Types.SMALLINT: return DataType.SMALLINT;
            case Types.FLOAT: return DataType.FLOAT;
            case Types.DOUBLE: return DataType.DOUBLE;
            case Types.DECIMAL: return DataType.DECIMAL;
            case Types.BOOLEAN: return DataType.BOOLEAN;
            case Types.DATE: return DataType.DATE;
            case Types.TIME: return DataType.TIME;
            case Types.TIMESTAMP: return DataType.TIMESTAMP;
            case Types.VARCHAR: return DataType.VARCHAR;
            case Types.CHAR: return DataType.CHAR;
            case Types.BINARY: return DataType.BINARY;
            case Types.VARBINARY: return DataType.VARBINARY;

            default:
                throw new IllegalStateException("Missing mapping for type " + type);
        }
    }
    
    
    static int getColumnType(DataType type) {

        switch (type) {
            case INTEGER: return Types.INTEGER; 
            case BIGINT: return Types.BIGINT;
            case TINYINT: return Types.TINYINT;
            case SMALLINT: return Types.SMALLINT;
            case FLOAT: return Types.FLOAT;
            case DOUBLE: return Types.DOUBLE;
            case DECIMAL: return Types.DECIMAL;
            case BOOLEAN: return Types.BOOLEAN;
            case DATE: return Types.DATE;
            case TIME: return Types.TIME;
            case TIMESTAMP: return Types.TIMESTAMP;
            case VARCHAR: return Types.VARCHAR;
            case CHAR: return Types.CHAR;
            case BINARY: return Types.BINARY;
            case VARBINARY: return Types.VARBINARY;

            default:
                throw new IllegalStateException("Missing mapping for type " + type);
        }
    }
    
    
    static void setPrepareStatementParameters(PreparedStatement preparedStatement, List<Param> params) 
            throws SQLException {

        for (int i = 0; i < params.size(); i++) {
            
            Param param = params.get(i);
            int parametrIndex = i + 1;
            byte[] bytes = param.getBytes().toByteArray();
            
            if (bytes.length == 0) {
                int columnType = getColumnType(param.getType());
                preparedStatement.setNull(parametrIndex, columnType);
                continue;
            }
            
            switch (param.getType()) {
                case INTEGER:
                    preparedStatement.setInt(parametrIndex, getInt(bytes));
                    break;
                    
                case BIGINT:
                    preparedStatement.setLong(parametrIndex, getLong(bytes));
                    break;
                    
                case TINYINT:
                    preparedStatement.setByte(parametrIndex, getByte(bytes));
                    break;
                    
                case SMALLINT:
                    preparedStatement.setShort(parametrIndex, getShort(bytes));
                    break;
                    
                case FLOAT:
                    preparedStatement.setFloat(parametrIndex, getFloat(bytes));
                    break;
                    
                case DOUBLE:
                    preparedStatement.setDouble(parametrIndex, getDouble(bytes));
                    break;
                    
                case DECIMAL:
                    preparedStatement.setBigDecimal(parametrIndex, getBigDecimal(bytes));
                    break;
                    
                case BOOLEAN:
                    preparedStatement.setBoolean(parametrIndex, getBoolean(bytes));
                    break;
                    
                case DATE:
                    preparedStatement.setDate(parametrIndex, getDate(bytes));
                    break;
                    
                case TIME:
                    preparedStatement.setTime(parametrIndex, getTime(bytes));
                    break;
                    
                case TIMESTAMP:
                    preparedStatement.setTimestamp(parametrIndex, getTimeStamp(bytes));
                    break;
                    
                case VARCHAR:
                case CHAR:
                    preparedStatement.setString(parametrIndex, getString(bytes));
                    break;
                    
                case BINARY:
                case VARBINARY:
                    preparedStatement.setBytes(parametrIndex, bytes);
                    break;
                
                default:
                    throw new IllegalStateException("Missing mapping for type " + param.getType());
            }
        }
    }
    
    private static final String expErr = "Expected size for '%s' is %d but was %d";
    
    private static void checkSize(byte[] bytes, String typeName, int expectedSize) {
        Assert.isTrue(bytes.length == expectedSize, String.format(expErr, typeName, expectedSize, bytes.length));
    }
    
    static int getInt(byte[] bytes) {
        checkSize(bytes, "int", INT_SIZE);
        return ByteBuffer.wrap(bytes).getInt();
    }
    
    static long getLong(byte[] bytes) {
        checkSize(bytes, "long", LONG_SIZE);
        return ByteBuffer.wrap(bytes).getLong();        
    }

    static byte getByte(byte[] bytes) {
        checkSize(bytes, "byte", BYTE_SIZE);
        return ByteBuffer.wrap(bytes).get();
    }

    static short getShort(byte[] bytes) {
        checkSize(bytes, "short", SHORT_SIZE);
        return ByteBuffer.wrap(bytes).getShort();
    }

    private static float getFloat(byte[] bytes) {
        checkSize(bytes, "float", INT_SIZE);
        return ByteBuffer.wrap(bytes).getFloat();
    }

    private static double getDouble(byte[] bytes) {
        checkSize(bytes, "double", LONG_SIZE);
        return ByteBuffer.wrap(bytes).getDouble();
    }

    private static BigDecimal getBigDecimal(byte[] bytes) {
        checkSize(bytes, "decimal", LONG_SIZE);
        double doubleValue = ByteBuffer.wrap(bytes).getDouble();
        return BigDecimal.valueOf(doubleValue);
    }

    private static boolean getBoolean(byte[] bytes) {
        checkSize(bytes, "boolean", BYTE_SIZE);
        return bytes[0] == 0x01;
    }

    private static Date getDate(byte[] bytes) {
        checkSize(bytes, "date", LONG_SIZE);
        long longValue = ByteBuffer.wrap(bytes).getLong();
        return new Date(longValue);
    }

    private static Time getTime(byte[] bytes) {
        checkSize(bytes, "time", LONG_SIZE);
        long longValue = ByteBuffer.wrap(bytes).getLong();
        return new Time(longValue);
    }

    private static Timestamp getTimeStamp(byte[] bytes) {
        checkSize(bytes, "timestamp", LONG_SIZE + INT_SIZE);
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        long timeValue = wrap.getLong();
        int nanoValue = wrap.getInt();
        Timestamp timestamp = new Timestamp(timeValue);
        timestamp.setNanos(nanoValue);
        return timestamp;
    }

    private static String getString(byte[] bytes) {
        return new String(bytes, Charset.forName("UTF-8"));
    }
}
