/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.socialbakers.phoenix.proxy.server;

import com.google.protobuf.ByteString;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.DataType;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

/**
 *
 * @author robert
 */
public class ValueTypeMapper {
    
    static final int BYTE_SIZE = Byte.SIZE / Byte.SIZE;     // 1B
    static final int SHORT_SIZE = Short.SIZE / Byte.SIZE;   // 2B
    static final int INT_SIZE = Integer.SIZE / Byte.SIZE;   // 4B
    static final int LONG_SIZE = Long.SIZE / Byte.SIZE;     // 8B
    
    
    static ByteString getValue(ResultSet rs, ResultSetMetaData meta,
            int column) throws SQLException, Exception {

        Object o = rs.getObject(column);
        if (o == null || rs.wasNull()) {
            return ByteString.EMPTY;
        }

        ByteBuffer b = null;
        int type = meta.getColumnType(column);
        
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
                // optimalization!
                return ByteString.copyFromUtf8((String) o);
//                b = ByteBuffer.wrap(ByteString.copyFromUtf8((String) o).toByteArray());
//                break;
                
            case Types.BINARY:
            case Types.VARBINARY:
                b = ByteBuffer.wrap((byte[])o);
                break;
                
            default:
                throw new Exception("Missing mapping for type " + type);
        }
        
        return ByteString.copyFrom(b.array());
    }

    static DataType getColumnType(int type) throws Exception {

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
                throw new Exception("Missing mapping for type " + type);
        }
    }
}
