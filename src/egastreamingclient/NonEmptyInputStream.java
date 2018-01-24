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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

/**
 * @author Lorber Sebastien <i>(lorber.sebastien@gmail.com)</i>
 */
public class NonEmptyInputStream extends FilterInputStream {

  /**
   * Once this stream has been created, do not consume the original InputStream 
   * because there will be one missing byte...
   * @param originalInputStream
   * @throws IOException
   * @throws EmptyInputStreamException
   */
  public NonEmptyInputStream(InputStream originalInputStream) throws IOException, EmptyInputStreamException {
    super( checkStreamIsNotEmpty(originalInputStream) );
  }


  /**
   * Permits to check the InputStream is empty or not
   * Please note that only the returned InputStream must be consummed.
   *
   * see:
   * http://stackoverflow.com/questions/1524299/how-can-i-check-if-an-inputstream-is-empty-without-reading-from-it
   *
   * @param inputStream
   * @return
   */
  private static InputStream checkStreamIsNotEmpty(InputStream inputStream) throws IOException, EmptyInputStreamException {
    PushbackInputStream pushbackInputStream = new PushbackInputStream(inputStream);
    int b;
    b = pushbackInputStream.read();
    if ( b == -1 ) {
      throw new EmptyInputStreamException("No byte can be read from stream " + inputStream);
    }
    pushbackInputStream.unread(b);
    return pushbackInputStream;
  }

  public static class EmptyInputStreamException extends RuntimeException {
    public EmptyInputStreamException(String message) {
      super(message);
    }
  }

}