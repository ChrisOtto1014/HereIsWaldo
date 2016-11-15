import java.io.InputStream;
import java.net.URL;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.sanselan.Sanselan;
import org.apache.sanselan.common.IImageMetadata;
import org.apache.sanselan.common.ImageMetadata.Item;
import org.xml.sax.Attributes;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class Main {

    private static final String BUCKET = "http://s3.amazonaws.com/waldo-recruiting";
    private static final ExecutorService executor3 = Executors.newFixedThreadPool(2);
    // TODO - mimic messaging service of sorts to hand-off from one process to another
    private static final BlockingQueue<Contents> sharedQueue = new LinkedBlockingQueue<>();

    // TODO - use database
    private static final Set<String> uniqueArtists = new TreeSet<>();
    private static final Set<Contents> uniqueContents = new HashSet<>();
    private static final Set<LocalDate> uniqueDates = new TreeSet<>();
    private static final Set<String> uniqueMakes = new TreeSet<>();
    private static final Set<String> uniqueModels = new TreeSet<>();

    public static void main(String[] args) throws Exception {

        // TODO - use executors for fail-safe behavior
        ExecutorService executor1 = Executors.newFixedThreadPool(1);
        ExecutorService executor2 = Executors.newFixedThreadPool(1);

        executor1.execute(new Consumer());
        executor2.execute(new Producer());
    }

    // TODO - regex
    private static Set<Contents> queryOnArtist(String artist) {
        return uniqueContents.stream().filter(c -> Objects.equals(artist, c.artist))
                .collect(Collectors.toCollection(TreeSet::new));
    }

    // TODO - more robust - allow "around noon", etc
    private static Set<Contents> queryOnDate(LocalDate date) {
        return uniqueContents.stream().filter(c -> Objects.equals(date, c.date))
                .collect(Collectors.toCollection(TreeSet::new));
    }

    // TODO - regex
    private static Set<Contents> queryOnMake(String make) {
        return uniqueContents.stream().filter(c -> Objects.equals(make, c.make))
                .collect(Collectors.toCollection(TreeSet::new));
    }

    // TODO - regex
    private static Set<Contents> queryOnModel(String model) {
        return uniqueContents.stream().filter(c -> Objects.equals(model, c.model))
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private static class Consumer implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    Contents contents = sharedQueue.take();
                    System.out.println(Thread.currentThread().getName() + ": Consumed: " + contents);
                    if (contents != null) {
                        executor3.execute(new Parser(contents));
                    }
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    private static class Contents {
        private static final String ARTIST = "Artist";
        private static final String CREATE_DATE = "Create Date";
        private static final String CREATE_DATE_PATTERN = "yyyy:MM:dd HH:mm:ss";
        private static final String MAKE = "Make";
        private static final String MODEL = "Model";
        String artist;
        LocalDate date;
        String filename;
        String lastModified;
        String make;
        String model;
        String size;

        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o instanceof Contents) {
                Contents that = (Contents) o;
                return this.filename.equals(that.filename);
            }
            return false;
        }

        public int hashCode() {
            return this.filename.hashCode();
        }

        public boolean isValid() {
            return this.filename != null;
        }

        public void parseExif() {
            try {
                String uri = BUCKET + "/" + filename;
                URL url = new URL(uri);
                InputStream stream = url.openStream();
                IImageMetadata metadata = Sanselan.getMetadata(stream, filename);
                System.out.println(Thread.currentThread().getName() + " Parsed: " + uri);
                for (Object o : metadata.getItems()) {
                    Item item = (Item) o;
                    String key = trim(item.getKeyword());
                    String txt = trim(item.getText());
                    switch (key) {
                    case ARTIST:
                        uniqueArtists.add(txt);
                        artist = txt;
                        System.out.println("  " + key + " : \"" + txt + "\"");
                        break;
                    case MAKE:
                        uniqueMakes.add(txt);
                        make = txt;
                        System.out.println("  " + key + " : \"" + txt + "\"");
                        break;
                    case MODEL:
                        uniqueModels.add(txt);
                        model = txt;
                        System.out.println("  " + key + " : \"" + txt + "\"");
                        break;
                    case CREATE_DATE:
                        System.out.println("  " + key + " : \"" + txt + "\"");
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(CREATE_DATE_PATTERN)
                                .withZone(ZoneId.systemDefault());
                        date = LocalDate.parse(txt, formatter);
                        uniqueDates.add(date);
                        System.out.println("  " + key + " : " + date);
                        break;
                    }
                }
                uniqueContents.add(this);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public String toString() {
            return filename + ", size = " + size + ", lastModified = " + lastModified;
        }

        private String trim(String s) {
            int i0 = 0;
            int i1 = s.length();
            if (s.charAt(0) == '\'') {
                i0++;
            }
            if (s.charAt(i1 - 1) == '\'') {
                i1--;
            }
            return s.substring(i0, i1);
        }
    }

    private static class Parser implements Runnable {

        private Contents contents;

        Parser(Contents c) {
            this.contents = c;
        }

        @Override
        public void run() {
            contents.parseExif();
        }
    }

    private static class Producer implements Runnable {

        @Override
        public void run() {
            try {
                SAXParserFactory spf = SAXParserFactory.newInstance();
                spf.setNamespaceAware(true);
                SAXParser saxParser = spf.newSAXParser();
                XMLReader xmlReader;
                xmlReader = saxParser.getXMLReader();
                xmlReader.setContentHandler(new MyContentHandler());
                xmlReader.setErrorHandler(new MyErrorHandler());
                xmlReader.parse(BUCKET);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * <pre>
         * <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
         *   <Name>waldo-recruiting</Name>
         *   <Prefix/>
         *   <Marker/>
         *   <MaxKeys>1000</MaxKeys>
         *   <IsTruncated>false</IsTruncated>
         *   <Contents>
         *     <Key>0003b8d6-d2d8-4436-a398-eab8d696f0f9.68cccdd4-e431-457d-8812-99ab561bf867.jpg</Key>
         *     <LastModified>2016-08-12T13:21:43.000Z</LastModified>
         *     <ETag>"0d275810374b5d169de000276d193224"</ETag>
         *     <Size>6306109</Size>
         *     <StorageClass>STANDARD</StorageClass>
         *   </Contents>
         *   ...
         * </ListBucketResult>
         * </pre>
         */
        private class MyContentHandler extends DefaultHandler {

            private static final String CONTENTS_NODE = "Contents";
            private static final String KEY_NODE = "Key";
            private static final String NAME_NODE = "Name";
            private Contents contents;
            private String currentNode = null;
            private String name;

            public void characters(char ch[], int start, int length) throws SAXException {
                String s = new String(ch, start, length);
                System.out.println(Thread.currentThread().getName() + ": characters " + s);
                if (NAME_NODE.equals(currentNode)) {
                    if (name == null)
                        name = s;
                    else
                        name += s;
                } else if (KEY_NODE.equals(currentNode)) {
                    if (contents.filename == null)
                        contents.filename = s;
                    else
                        contents.filename += s;
                }
            }

            public void endDocument() throws SAXException {
                System.out.println(Thread.currentThread().getName() + ": " + name);
                System.out.println(Thread.currentThread().getName() + ": endDocument");
            }

            public void endElement(String uri, String localName, String qName) throws SAXException {
                System.out.println(Thread.currentThread().getName() + ": endElement " + qName);
                if (CONTENTS_NODE.equals(qName)) {
                    System.out.println(Thread.currentThread().getName() + ": Produced " + contents);
                    try {
                        if (contents.isValid())
                            sharedQueue.put(contents);
                        else
                            System.out.println(Thread.currentThread().getName() + ": Invalid " + contents);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    contents = null;
                }
                currentNode = null;
            }

            public void startDocument() throws SAXException {
                System.out.println(Thread.currentThread().getName() + ": startDocument");
            }

            public void startElement(String uri, String localName, String qName, Attributes attributes)
                    throws SAXException {
                System.out.println(Thread.currentThread().getName() + ": startElement " + qName);

                currentNode = qName;
                if (CONTENTS_NODE.equals(qName)) {
                    currentNode = CONTENTS_NODE;
                    contents = new Contents();
                }
            }
        }

        private class MyErrorHandler implements ErrorHandler {

            public void error(SAXParseException spe) throws SAXException {
                String message = Thread.currentThread().getName() + ": Error: " + getParseExceptionInfo(spe);
                throw new SAXException(message);
            }

            public void fatalError(SAXParseException spe) throws SAXException {
                String message = Thread.currentThread().getName() + ": Fatal Error: " + getParseExceptionInfo(spe);
                throw new SAXException(message);
            }

            public void warning(SAXParseException spe) throws SAXException {
                System.err.println(Thread.currentThread().getName() + ": Warning: " + getParseExceptionInfo(spe));
            }

            private String getParseExceptionInfo(SAXParseException spe) {
                String systemId = spe.getSystemId();
                if (systemId == null) {
                    systemId = "null";
                }
                return "URI=" + systemId + " Line=" + spe.getLineNumber() + ": " + spe.getMessage();
            }
        }
    }
}
