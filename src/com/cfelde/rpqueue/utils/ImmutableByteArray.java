/*
 * Copyright 2013 Christian Felde (cfelde [at] cfelde [dot] com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.cfelde.rpqueue.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;

/**
 * @author cfelde
 */
public final class ImmutableByteArray {
    private final byte[] payload;
    private final int hash;
    
    public ImmutableByteArray(byte[] payload) {
        if (payload == null)
            throw new NullPointerException();
        
        this.payload = CommonUtils.copy(payload);
        this.hash = 73 * 7 + Arrays.hashCode(this.payload);
    }

    /**
     * Returns a copy of the associated payload byte array.
     * 
     * @return Byte array representing associated payload
     */
    public byte[] getPayload() {
        return CommonUtils.copy(payload);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        
        final ImmutableByteArray other = (ImmutableByteArray) obj;
        if (this.hash != other.hash) {
            return false;
        }
        
        if (!Arrays.equals(this.payload, other.payload)) {
            return false;
        }
        
        return true;
    }
    
    public static ImmutableByteArray fromString(String string) {
        return new ImmutableByteArray(string.getBytes(CommonUtils.getCharset()));
    }
    
    public static String toString(ImmutableByteArray payload) {
        return new String(payload.payload, CommonUtils.getCharset());
    }
    
    public static ImmutableByteArray fromLong(long l) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(l);
        
        return new ImmutableByteArray(buffer.array());
    }
    
    public static long toLong(ImmutableByteArray payload) {
        byte[] b = payload.payload;
        
        if (b.length != 8)
            throw new IllegalArgumentException("Given payload doesn't fit long");
        
        ByteBuffer buffer = ByteBuffer.wrap(b);
        
        return buffer.getLong();
    }
    
    public static ImmutableByteArray fromUUID(UUID uuid) {
        ByteBuffer buffer = ByteBuffer.allocate(8 * 2);
        
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        
        return new ImmutableByteArray(buffer.array());
    }
    
    public static UUID toUUID(ImmutableByteArray payload) {
        byte[] b = payload.payload;
        
        if (b.length != 8 * 2)
            throw new IllegalArgumentException("Given payload doesn't fit UUID");
        
        ByteBuffer buffer = ByteBuffer.wrap(b);
        
        return new UUID(buffer.getLong(), buffer.getLong());
    }
}
