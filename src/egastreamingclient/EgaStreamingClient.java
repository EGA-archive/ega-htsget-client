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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.api.client.auth.oauth2.PasswordTokenRequest;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.http.BasicAuthentication;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import htsjdk.samtools.seekablestream.SeekableHTTPStream;
import htsjdk.samtools.seekablestream.SeekableStream;
import htsjdk.samtools.util.Tuple;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;

/**
 *
 * @author asenf
 */
public class EgaStreamingClient {
    private static final int VERSION_MAJOR = 1;
    private static final int VERSION_MINOR = 1;
    
    private static OkHttpClient client = null;

     // EGA AAI
    private static String TOKEN_SERVER_URL = "https://ega.ebi.ac.uk:8443/ega-openid-connect-server/token";
    private static final String AUTHORIZATION_SERVER_URL = "https://ega.ebi.ac.uk:8443/ega-openid-connect-server/authorize";
   
    // Tokens
    private static String refreshToken;

    /** OAuth 2 scope. */
    private static String SCOPE = "openid";

    /** Global instance of the HTTP transport. */
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

    /** Global instance of the JSON factory. */
    static final JsonFactory JSON_FACTORY = new JacksonFactory();
    
    private static void error(String message) {
        System.err.println(message);
        System.exit(1);
    }
    private static enum Format {
        BAM,
        CRAM,
        VCF,
        BCF
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws NoSuchAlgorithmException, KeyManagementException {
        // EGA Ticket Endpoint as defaults - overwritten if parameters are specified
        String endpointUrl = "https://ega.ebi.ac.uk:8051/elixir/data/tickets/files/";
        String vcfEndpointUrl = "https://ega.ebi.ac.uk:8051/elixir/data/tickets/variants/";
        
        client = SSLUtilities.getUnsafeOkHttpClient();

        Params params = new Params();
        JCommander jc = new JCommander(params);
        jc.parse(args);
        
        // Print Help
        if (params.help) {
            jc.usage();
            return;
        }
        // Print Version
        if (params.version) {
            System.out.println("Version: " + VERSION_MAJOR + "." + VERSION_MINOR);
            return;
        }
        
        // Handle tokens, if necessary [requires access to an EGA AAI client]
        if (params.tokenUser!=null && params.tokenPass!=null) try {
            if (params.tokenUrl!=null)
                TOKEN_SERVER_URL = params.tokenUrl;
            String user = fileTest(params.tokenUser);
            String pass = fileTest(params.tokenPass);
            TokenResponse token = authorize(user, pass);
            params.oauthToken = token.getAccessToken();
            refreshToken = token.getRefreshToken();
        } catch (Exception ex) {System.out.println("ERROR " + ex.toString());}
        
        // Do the work
        try {
            // 1 - Get the Ticket!
            Query query = new Query();
            query.sequence = params.referenceName;
            query.start = params.start;
            query.end = params.stop;
            if (params.format==Format.VCF)
                endpointUrl = vcfEndpointUrl;
            if (params.endpointUrl!=null)
                endpointUrl = params.endpointUrl;
            String sURL = formatURL(endpointUrl, params.datasetId, query, params.format);
            URL url = new URL(sURL);
            if (params.debug) System.out.println("Version " + VERSION_MAJOR + "." + VERSION_MINOR + " Getting Ticket URL " + sURL);
            TicketResponse r = getTicketOk(url, fileTest(params.oauthToken), params.printTicket);

            // 2 - Download the Data for each URL in the ticket
            IOException exception = null;
            if (r!=null && r.urls!=null) { // Ticket is not NULL
                exception = null;

                OutputStream outputStream; // Output File
                if (params.outputFile == null) outputStream = new BufferedOutputStream(System.out);
                else outputStream = new FileOutputStream(params.outputFile);
                try {
                    /*
                     * Iterate through Each Ticket URL!
                     */
                    long total = 0;
                    for (TicketResponse.URL_OBJECT uo : r.urls) {
                        if (params.debug) System.out.println("Version " + VERSION_MAJOR + "." + VERSION_MINOR + " Opening Data URL " + uo.url);
                        if (uo.url.startsWith("data")) {
                            // Embedded Data - write immediately to output
                            final byte[] bytes = TicketResponse.fromDataURI(new URI(uo.url));
                            total += bytes.length;
                            outputStream.write(bytes);
                        } else {
                            // Data URL - write to temp file, until successful - including ReTries
                            int tryCount = params.retries;
                            Exception e_ = null;
                            do {
                                e_ = null;
                                try {
                                    // (1) Set up Temp File
                                    File tempFile = File.createTempFile("tempfile", ".tmp");
                                    tempFile.deleteOnExit(); // just in case of exception.
                                    FileOutputStream fos = new FileOutputStream(tempFile);

                                    // (2) Get Input Stream (with retries)
                                    InputStream is = getInputStreamFromTicketURL(uo, params.bufferSize, params.debug);
                                    is = new BufferedBackgroundInputStream(is);

                                    // (3) Write Input Stream to file
                                    final byte[] buffer = new byte[params.bufferSize];
                                    int bytesRead;
                                    while ((bytesRead = is.read(buffer)) > 0) {
                                        //total += bytesRead;
                                        fos.write(buffer, 0, bytesRead);
                                    }
                                    is.close();
                                    fos.close();

                                    // (4) Copy Temp file to Output File, delete Temp File
                                    Path path = tempFile.toPath();
                                    long copy = Files.copy(path, outputStream);
                                    total += copy;
                                    tempFile.delete();
                                } catch (Exception e) {
                                    System.out.println(e.toString());
                                    e_ = e;
                                }
                            } while (tryCount-- > 0 && e_!=null);
                        }
                        if (params.debug) System.out.println("Version " + VERSION_MAJOR + "." + VERSION_MINOR + " Total bytes read from stream: " + total);
                    }
                } catch (IOException e) {
                    System.out.println(e.toString());
                }
                outputStream.close();
            } else {
                System.out.println("Ticket response is null.");
            }
            
            return;
        } catch (Throwable th) {System.out.println(th.toString());return;}
    }
    
    /*
     *  ************************************************************************ 
     *  ****                                                                **** 
     *  ************************************************************************ 
     */
    
    private static String fileTest(String string) {
        if (string==null) return null;
        if (string.toLowerCase().startsWith(("file://"))) {
            URL url;
            try {
                url = new URL(string);
                URI uri = url.toURI();

                if(uri.getAuthority() != null && uri.getAuthority().length() > 0) {
                    uri = (new URL("file://" + string.substring("file:".length()))).toURI();
                }
                
                File file = new File(uri);
                BufferedReader br = new BufferedReader(new FileReader(file));
                
                String fileString = br.readLine();
                br.close();
                
                return fileString;
            } catch (Exception ex) {System.out.println("ERROR " + ex.toString());}
        } else {
            return string;
        }
        
        return null;
    }
    
    // This uses a test client setup at the EGA AAI - continued existance of this
    // client is not guaranteed. Preference: use an OAuth2 Bearer token instead.
    private static TokenResponse authorize(String username, String pass) throws Exception {
        
        TokenResponse response = 
            new PasswordTokenRequest(HTTP_TRANSPORT, 
                                   JSON_FACTORY, 
                                   new GenericUrl(TOKEN_SERVER_URL), 
                                   username.toString(), 
                                   pass.toString())
                .setGrantType("password")
                .setClientAuthentication( 
                    new BasicAuthentication("f20cd2d3-682a-4568-a53e-4262ef54c8f4",
                                            "AMenuDLjVdVo4BSwi0QD54LL6NeVDEZRzEQUJ7hJOM3g4imDZBHHX0hNfKHPeQIGkskhtCmqAJtt_jm7EKq-rWw")
            ).execute(); 
        
        return response;
    }
    private static void refresh() throws Exception {
        
    }
    
    private static String formatURL(String base, String accession, Query query, Format format) {
        String url = String.format("%s%s?format=%s&referenceName=%s", base, accession, format, query.sequence);
        if (query.start < 0) query.start = 0;
        url = String.format(url + "&start=%d", query.start);
        if (query.end > 0) url = String.format(url + "&end=%d", query.end);
        System.out.println(url);
        return url;
    }

    private static TicketResponse getTicketOk(URL url, String oauthToken, boolean printTicket) 
            throws IOException, EndpointException, KeyManagementException, NoSuchAlgorithmException, VersionException {

        Request requestRequest = null;
        
        if (oauthToken!=null && oauthToken.length()>0) {
            requestRequest = new Request.Builder()
                .url(url)
                .addHeader("Authorization", "Bearer " + oauthToken)
                .build();
        } else {
            requestRequest = new Request.Builder()
                .url(url)
                .build();
        }
        
        Response response = null;
        int tryCount = 9;
        while (tryCount-->0 && (response == null || !response.isSuccessful())) {
            try {
                response = client.newCall(requestRequest).execute();
            } catch (Exception ex) {
                System.out.println(ex.toString());
                return null;
            }
        }
        ResponseBody body = response.body();

        final InputStream inputStream = body.byteStream();
        InputStreamReader reader = new InputStreamReader(new BufferedInputStream(inputStream), "ASCII");
        Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
        HtsgetResponse htsgetResponse = gson.fromJson(reader, HtsgetResponse.class);
        TicketResponse ticketResponse = htsgetResponse.htsget;
        if (printTicket) System.out.println(gson.toJson(htsgetResponse));

        body.close();
        response.close();

        return ticketResponse;
    }

    private static class EndpointException extends Exception {
        int code;

        public EndpointException(int code) {
            this.code = code;
        }
    }

    private static class VersionException extends Exception {
        String version;

        public VersionException(String code) {
            this.version = version;
        }
    }
    
    private static InputStream getInputStreamFromTicketURL(TicketResponse.URL_OBJECT uo, final int bufSize, boolean debug) 
                throws IOException, URISyntaxException, ParseException {
        
        boolean secure = uo.url.toLowerCase().startsWith("https");
        boolean auth = false;
        String authToken = "";
        if (uo.headers != null) {
            auth = (uo.headers.containsKey("authorization") || uo.headers.containsKey("Authorization"));
            if (auth) {
                authToken = uo.headers.containsKey("authorization")?
                        uo.headers.get("authorization"):
                        uo.headers.get("Authorization");
            }
        }

        final Tuple<Long, Long> range = uo.getRange();

        int reTries = 5; 
        InputStream is_ = null;
        /*
         * Range is specified - use EGA/Samtools Seekable Streams
         */
        if (range != null) {
            if (debug) System.out.println("Version " + VERSION_MAJOR + "." + VERSION_MINOR + " Range Specified: " + range.a + "-" + range.b);
            SeekableStream stream = null;
            Exception e = null;                    
            do {
                e = null;
                try {
                    if (debug) System.out.println("Version " + VERSION_MAJOR + "." + VERSION_MINOR + " Opening Seekable URL: " + uo.url.toString());
                    int tryCount = 4;
                    Exception e_ = null;
                    do {
                        e_ = null;
                        try {
                            if (!secure) {      // Original Implementation - Standard HTTP
                                stream = new SeekableHTTPStream(new URL(uo.url));
                            } else {            // HTTPS Secured
                                if (!auth) {    // Using (assumed) Basic Auth
                                    stream = new EgaSeekableHTTPStream(new URL(uo.url), authToken);
                                } else {        // Using Outh2 Bearer Tokens (EGA)
                                    stream = new EgaSeekableHTTPStreamOAuth(new URL(uo.url), authToken);
                                }
                            }
                        } catch (Exception ex) {
                            try {Thread.sleep(500);} catch (InterruptedException ex1) {;}
                            System.out.println("Client Stream Instatiation ERROR: " + ex.toString());
                            e_ = ex;
                        }  
                    } while (tryCount-- > 0 && stream==null && e_!=null);
                    stream.seek(range.a);
                    long size = range.b - range.a + 1;

                    InputStream is = new LimitedInputStream(new BufferedInputStream(stream, bufSize), size);
                    if (debug) System.out.println("Version " + VERSION_MAJOR + "." + VERSION_MINOR + " Trying to Read from Response Stream.");
                    is_ = new NonEmptyInputStream(is);
                    if (is_!=null) {
                        InputStream is__ = null;
                        if (size > 0L)
                            is__ = new LimitedInputStream(new BufferedInputStream(is_, bufSize), size);
                        else
                            is__ = new BufferedInputStream(is_, bufSize);
                        return is__;
                    }
                } catch (Exception ex) {
                    System.out.println("Exception (" + reTries + ") opening URL " + uo.url);
                    try {Thread.sleep(2000);} catch (InterruptedException ex1) {;}
                    e = ex;
                    reTries--;
                }
            } while (e!=null && reTries > 0);

        } else {
        /*
         * Range is not specified - read entire URL using OkHTTP3 client
         */
        if (debug) System.out.println("Version " + VERSION_MAJOR + "." + VERSION_MINOR + " Range Not Specified.");
            long size = getContentLength(uo);
            InputStream is = null;

            Request requestRequest = null;
            URL url = new URL(uo.url);

            if (auth) {
                requestRequest = new Request.Builder()
                    .url(url)
                    .addHeader("Authorization", authToken)
                    .build();
            } else {
                requestRequest = new Request.Builder()
                    .url(url)
                    .build();
            }

            InputStream inputStream = null;
            Exception e = null;
            do {
                e = null;
                try {
                    Response streamingResponse = null;
                    int tryCount = 4;
                    while (tryCount-->0 && (streamingResponse == null || !streamingResponse.isSuccessful())) {
                        try {
                            if (debug) System.out.println("Version " + VERSION_MAJOR + "." + VERSION_MINOR + " Executing URL: " + requestRequest.url().toString());
                            streamingResponse = client.newCall(requestRequest).execute();
                        } catch (Exception ex) {
                            try {Thread.sleep(500);} catch (InterruptedException ex1) {;}
                            System.out.println("Client Execute ERROR: " + ex.toString());
                        }
                    }
                    ResponseBody body = streamingResponse.body();
                    inputStream = body.byteStream();

                    if (debug) System.out.println("Version " + VERSION_MAJOR + "." + VERSION_MINOR + " Trying to Read from Response Stream.");
                    is_ = new NonEmptyInputStream(inputStream);
                    if (is_!=null) {
                        InputStream is__ = null;
                        if (size > 0L && !(uo.url.contains("start")||uo.url.contains("end")) )
                            is__ = new LimitedInputStream(new BufferedInputStream(is_, bufSize), size);
                        else
                            is__ = new BufferedInputStream(is_, bufSize);
                        return is__;
                    }
                } catch (Exception ex) {
                    System.out.println("Exception (" + reTries + ") opening/reading URL " + uo.url);
                    try {Thread.sleep(2000);} catch (InterruptedException ex1) {;}
                    e = ex;
                    reTries--;
                }
            } while (e!=null && reTries > 0);

        }
        
        return null;
    }    
    
    private static InputStream getSimpleInputStreamFromTicketURL(TicketResponse.URL_OBJECT uo, final int bufSize, boolean debug) 
                throws IOException, URISyntaxException, ParseException {
        
        HttpURLConnection connection = null;

        URL url = new URL(uo.url);
        boolean secure = uo.url.toLowerCase().startsWith("https");
        boolean auth = false;
        if (uo.headers != null) {
            auth = (uo.headers.containsKey("authorization") || uo.headers.containsKey("Authorization"));
        }
        
        try {   // Handle HTTPS anf various Auth methods
            connection = secure ?
                    (HttpsURLConnection) url.openConnection() :
                    (HttpURLConnection) url.openConnection();
            if (auth) {
                String auth_ = uo.headers.containsKey("authorization") ?
                        uo.headers.get("authorization") :
                        uo.headers.get("Authorization");
                if (auth_ != null) {
                    connection.setRequestProperty("Authorization", auth_);
                }
            }
            connection.setReadTimeout(10000);
            return connection.getInputStream();
        } catch (IOException e) {
            return null;
        }
    }    
    
    /**
     * Try to request content length from the URL via HTTP HEAD method.
     * @param url the URL to request about
     * @return number of bytes to expect from the URL or -1 if unknown.
     */
    private static long getContentLength(TicketResponse.URL_OBJECT uo) throws MalformedURLException {
        HttpURLConnection connection = null;

        URL url = new URL(uo.url);
        boolean secure = uo.url.toLowerCase().startsWith("https");
        boolean auth = false;
        if (uo.headers != null) {
            auth = (uo.headers.containsKey("authorization") || uo.headers.containsKey("Authorization"));
        }
        
        try {   // Handle HTTPS anf various Auth methods
            connection = secure ?
                    (HttpsURLConnection) url.openConnection() :
                    (HttpURLConnection) url.openConnection();
            if (auth) {
                String auth_ = uo.headers.containsKey("authorization") ?
                        uo.headers.get("authorization") :
                        uo.headers.get("Authorization");
                if (auth_ != null) {
                    connection.setRequestProperty("Authorization", auth_);
                }
            }
            connection.setRequestMethod("HEAD");
            connection.getInputStream();
            long contentLengthLong = connection.getContentLengthLong();
            Map<String, List<String>> headerFields = connection.getHeaderFields();
            return connection.getContentLengthLong();
        } catch (IOException e) {
            return -1L;
        } finally {
            connection.disconnect();
        }
    }
    
    /*
     * Parameters 
     */
    @Parameters
    static class Params {
        @Parameter(names = {"--endpoint-url"}, description = "An endpoint URL to be used for querying")
        String endpointUrl;

        @Parameter(names = {"--endpoint-name"}, description = "Endpoint name to be used for querying, resolved via configuration file")
        String endpointName;

        @Parameter(names = {"--dataset-id"}, description = "Dataset id to request")
        String datasetId;

        @Parameter(names = {"--reference-name"}, description = "Reference sequence name to request")
        String referenceName;

        @Parameter(names = {"--alignment-start"}, description = "Alignment start for genomic query")
        int start=0;

        @Parameter(names = {"--alignment-stop"}, description = "Alignment end for genomic query")
        int stop=0;

        @Parameter(names = {"--format"}, description = "Format : BAM or CRAM or VCF")
        Format format=Format.BAM;

        @Parameter(names = {"--output-file"}, description = "Output file to write received data, omit for STDOUT")
        File outputFile;

        @Parameter(names = {"--print-ticket"}, description = "Print json ticket before receiving data")
        boolean printTicket = false;

        @Parameter(names = {"--buffer-size"}, description = "The buffer size to be used for downloaded data")
        int bufferSize=1024*1024;

        @Parameter(names = {"--retries"}, description = "The number of tries before declaring failure")
        int retries=3;

        @Parameter(names = {"--reference-fasta-file"}, description = "Reference fasta file to be used when reading CRAM stream")
        File refFile;

        @Parameter(names = {"--oauth-token"}, description = "EGA or ELIXIR OAuth2 Access Token (use 'file://' to specify a token file)")
        String oauthToken;

        @Parameter(names = {"--token-url"}, description = "Token Granting Server (EGA or ELIXIR) URL")
        String tokenUrl;
        
        @Parameter(names = {"--token-user"}, description = "EGA or ELIXIR Username (use 'file://' to specify a username file)")
        String tokenUser;

        @Parameter(names = {"--token-pass"}, description = "EGA or ELIXIR Password (use 'file://' to specify a password file)")
        String tokenPass;

        @Parameter(names = {"--help"}, description = "Print Help", help = true)
        boolean help = false;

        @Parameter(names = {"--version"}, description = "Print Version Number", help = true)
        boolean version = false;

        @Parameter(names = {"--debug"}, description = "Print Some Debug Information", help = true)
        boolean debug = false;
    }

}
