import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

    public static void main(String[] args) throws Exception {

        BlockingQueue<Contents> sharedQueue = new LinkedBlockingQueue<>();

        Thread producerThread = new Thread(new Producer(sharedQueue));
        Thread consumerThread = new Thread(new Consumer(sharedQueue));

        producerThread.start();
        consumerThread.start();
    }

    private static class Consumer implements Runnable {

        private final BlockingQueue<Contents> sharedQueue;

        public Consumer(BlockingQueue<Contents> sharedQueue) {
            this.sharedQueue = sharedQueue;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Contents contents = sharedQueue.take();
                    System.out.println("Consumed: " + contents);
                    if (contents != null) {
                        contents.parseExif();
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
        private static final String MAKE = "Make";
        private static final String MODEL = "Model";
        String filename;
        String lastModified;
        String size;

        public void parseExif() {
            try {
                String uri = BUCKET + "/" + filename;
                URL url = new URL(uri);
                InputStream stream = url.openStream();
                IImageMetadata metadata = Sanselan.getMetadata(stream, filename);
                System.out.println(uri);
                for (Object o : metadata.getItems()) {
                    Item item = (Item) o;
                    String key = item.getKeyword();
                    switch (key) {
                    case ARTIST:
                    case CREATE_DATE:
                    case MAKE:
                    case MODEL:
                        System.out.println("  " + item.getKeyword() + " : " + item.getText());
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public String toString() {
            return filename + ", size = " + size + ", lastModified = " + lastModified;
        }
    }

    private static class Producer implements Runnable {

        private final BlockingQueue<Contents> sharedQueue;

        public Producer(BlockingQueue<Contents> sharedQueue) {
            this.sharedQueue = sharedQueue;
        }

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
                    System.out.println(Thread.currentThread().getName() + ": produce " + contents);
                    try {
                        sharedQueue.put(contents);
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
