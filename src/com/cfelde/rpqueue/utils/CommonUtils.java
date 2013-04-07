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

import java.nio.charset.Charset;

/**
 * @author cfelde
 */
public class CommonUtils {
    public static byte[] copy(byte[] a) {
        if (a == null)
            return null;
        
        byte[] b = new byte[a.length];
        System.arraycopy(a, 0, b, 0, a.length);
        return b;
    }
    
    public static Charset getCharset() {
        return Charset.forName("UTF-8");
    }
}
