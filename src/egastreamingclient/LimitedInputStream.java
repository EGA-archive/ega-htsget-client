/*
 * Copyright 2017 ELIXIR EGA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package egastreamingclient;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by vadim on 12/05/2016.
 */
class LimitedInputStream extends InputStream {
    InputStream delegate;
    long size;
    long read = 0;

    public LimitedInputStream(InputStream delegate, long size) {
        this.delegate = delegate;
        this.size = size;
    }

    @Override
    public int read() throws IOException {
        if (read >= size) {
            return -1;
        }
        int result = delegate.read();
        if (result == -1)
            throw new IOException("The stream is incomplete, expected " + size + " but read " + read + " bytes.");
        read++;
        return result;
    }

    @Override
    public String toString() {
        return String.format("Limited stream: size=%d, read=%d, delegate=%s", size, read, delegate);
    }
}
