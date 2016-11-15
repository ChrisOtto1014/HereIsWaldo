import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.sanselan.Sanselan;
import org.apache.sanselan.common.IImageMetadata;
import org.xml.sax.Attributes;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

public class Main {

    private static final String BUCKET = "http://s3.amazonaws.com/waldo-recruiting";

    public static void main(String[] args) throws Exception {
        SAXParserFactory spf = SAXParserFactory.newInstance();
        spf.setNamespaceAware(true);
        SAXParser saxParser = spf.newSAXParser();
        XMLReader xmlReader = saxParser.getXMLReader();
        xmlReader.setContentHandler(new MyContentHandler());
        xmlReader.setErrorHandler(new MyErrorHandler());
        xmlReader.parse(BUCKET);
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
    private static class MyContentHandler extends DefaultHandler {

        private static final String NAME_NODE = "Name";
        private static final String CONTENTS_NODE = "Contents";
        private static final String KEY_NODE = "Key";
        private String currentNode = null;
        private String name;
        private List<Contents> contentsList = new ArrayList<>();
        private Contents contents;

        public void startDocument() throws SAXException {
            System.out.println("startDocument");
        }

        public void endDocument() throws SAXException {
            System.out.println(name);
            System.out.println(contentsList.size());
            for (Contents c : contentsList) {
                System.out.println(c);
                c.dump();
            }
            System.out.println("endDocument");
        }

        public void startElement(String uri, String localName, String qName, Attributes attributes)
                throws SAXException {
            System.out.println("startElement :" + qName);

            currentNode = qName;
            if (CONTENTS_NODE.equals(qName)) {
                currentNode = CONTENTS_NODE;
                contents = new Contents();
            }
        }

        public void endElement(String uri, String localName, String qName) throws SAXException {
            System.out.println("endElement :" + qName);
            if (CONTENTS_NODE.equals(qName)) {
                contentsList.add(contents);
                contents = null;
            }
            currentNode = null;
        }

        public void characters(char ch[], int start, int length) throws SAXException {
            String s = new String(ch, start, length);
            System.out.println("characters: " + s);
            if (NAME_NODE.equals(currentNode)) {
                name = s;
            } else if (KEY_NODE.equals(currentNode)) {
                contents.filename = s;
            }
        }
    }

    private static class Contents {
        String filename;
        String lastModified;
        String size;

        public String toString() {
            return filename + ", size = " + size + ", lastModified = " + lastModified;
        }

        public void dump() {
            try {
                String uri = BUCKET + "/" + filename;
                URL url = new URL(uri);
                InputStream stream = url.openStream();
                IImageMetadata metadata = Sanselan.getMetadata(stream, filename);
                System.out.println(metadata);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class MyErrorHandler implements ErrorHandler {

        private String getParseExceptionInfo(SAXParseException spe) {
            String systemId = spe.getSystemId();
            if (systemId == null) {
                systemId = "null";
            }
            return "URI=" + systemId + " Line=" + spe.getLineNumber() + ": " + spe.getMessage();
        }

        public void warning(SAXParseException spe) throws SAXException {
            System.err.println("Warning: " + getParseExceptionInfo(spe));
        }

        public void error(SAXParseException spe) throws SAXException {
            String message = "Error: " + getParseExceptionInfo(spe);
            throw new SAXException(message);
        }

        public void fatalError(SAXParseException spe) throws SAXException {
            String message = "Fatal Error: " + getParseExceptionInfo(spe);
            throw new SAXException(message);
        }
    }
}
