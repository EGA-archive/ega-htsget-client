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

import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by vadim on 27/09/2016.
 */
class Query {
    public String sequence;
    public long start;
    public long end;

    String toQueryString() {
        return String.format("%s:%d-%d", sequence, start, end);
    }

    static Query fromString(String s) throws ParseException {
        Pattern pattern = Pattern.compile("^([^:]+):(\\d+)-(\\d+)$]");
        Matcher matcher = pattern.matcher(s);
        if (matcher.matches() && matcher.groupCount() == 3) {
            Query query = new Query();
            query.sequence = matcher.group(1);
            query.start = Integer.valueOf(matcher.group(2));
            query.end = Integer.valueOf(matcher.group(3));
            return query;
        }
        throw new ParseException(s, 0);
    }
}
