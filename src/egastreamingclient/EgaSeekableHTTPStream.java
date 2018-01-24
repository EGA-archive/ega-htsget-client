/*
 * Copyright 2016 ELIXIR EGA
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

import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.HttpUtils;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;

/**
 * @author jrobinso
 */
public class EgaSeekableHTTPStream extends SeekableStream {

    private long position = 0;
    private long contentLength = -1;
    private final URL url;
    private final Proxy proxy;
    private final String auth;

    public EgaSeekableHTTPStream(final URL url) {
        this(url, null, null);
    }

    public EgaSeekableHTTPStream(final URL url, String auth) {
        this(url, null, auth);
    }

    public EgaSeekableHTTPStream(final URL url, Proxy proxy) {
        this(url, proxy, null);
    }
    
    public EgaSeekableHTTPStream(final URL url, Proxy proxy, String auth) {
        this(url, proxy, auth, -1);
    }
    
    public EgaSeekableHTTPStream(final URL url, Proxy proxy, String auth, long fileSize) {

        this.proxy = proxy;
        this.url = url;
        this.auth = auth;
        this.contentLength = fileSize;

        // Try to get the file length
        // Note: This also sets setDefaultUseCaches(false), which is important
        final String contentLengthString = HttpUtils.getHeaderField(url, "Content-Length");
        if (contentLengthString != null && contentLength == -1) {
            try {
                contentLength = Long.parseLong(contentLengthString);
            }
            catch (NumberFormatException ignored) {
                System.err.println("WARNING: Invalid content length (" + contentLengthString + "  for: " + url);
                contentLength = -1;
            }
        }
    }

    public long position() {
        return position;
    }

    public long length() {
        return contentLength;
    }

    @Override
    public long skip(long n) throws IOException {
        long bytesToSkip = Math.min(n, contentLength - position);
        position += bytesToSkip;
        return bytesToSkip;
    }

    public boolean eof() throws IOException {
        return contentLength > 0 && position >= contentLength;
    }

    public void seek(final long position) {
        this.position = position;
    }

    public int read(byte[] buffer, int offset, int len) throws IOException {

        if (offset < 0 || len < 0 || (offset + len) > buffer.length) {
            throw new IndexOutOfBoundsException("Offset="+offset+",len="+len+",buflen="+buffer.length);
        }
        if (len == 0 || position == contentLength) {
            if (position >= contentLength) {
                return -1;
            }
            return 0;
        }

        HttpURLConnection connection = null;
        InputStream is = null;
        String byteRange = "";
        int n = 0;
        try {
            connection = proxy == null ?
                    (HttpURLConnection) url.openConnection() :
                    (HttpURLConnection) url.openConnection(proxy);
            if (auth!=null && auth.length() > 0) {
                // Java bug : http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6459815
                String encoding = new sun.misc.BASE64Encoder().encode (auth.getBytes());
                encoding = encoding.replaceAll("\n", "");  
                String basicAuth = "Basic " + encoding;
                connection.setRequestProperty ("Authorization", basicAuth);
            }

            long endRange = position + len - 1;
            // IF we know the total content length, limit the end range to that.
            if (contentLength > 0) {
                endRange = Math.min(endRange, contentLength);
            }
            byteRange = "bytes=" + position + "-" + endRange;
            connection.setRequestProperty("Range", byteRange);

            connection.setConnectTimeout(120000);
            connection.setReadTimeout(180000);
            is = connection.getInputStream();

            while (n < len) {
                int count = is.read(buffer, offset + n, len - n);
                if (count < 0) {
                    if (n == 0) {
                        return -1;
                    } else {
                        break;
                    }
                }
                n += count;
            }

            position += n;

            return n;
        }

        catch (IOException e) {
            // THis is a bit of a hack, but its not clear how else to handle this.  If a byte range is specified
            // that goes past the end of the file the response code will be 416.  The MAC os translates this to
            // an IOException with the 416 code in the message.  Windows translates the error to an EOFException.
            //
            //  The BAM file iterator  uses the return value to detect end of file (specifically looks for n == 0).
            if (e.getMessage().contains("416") || (e instanceof EOFException)) {
                if (n == 0) {
                    return -1;
                } else {
                    position += n;
                    // As we are at EOF, the contentLength and position are by definition =
                    contentLength = position;
                    return n;
                }
            } else {
                throw e;
            }

        }

        finally {
            if (is != null) {
                is.close();
            }
            if (connection != null) {
                connection.disconnect();
            }
        }
    }


    public void close() throws IOException {
        // Nothing to do
    }


    public int read() throws IOException {
    	byte []tmp=new byte[1];
    	read(tmp,0,1);
    	return (int) tmp[0] & 0xFF; 
    }

    @Override
    public String getSource() {
        return url.toString();
    }
}