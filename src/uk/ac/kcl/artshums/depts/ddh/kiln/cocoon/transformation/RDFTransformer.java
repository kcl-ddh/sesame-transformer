package uk.ac.kcl.artshums.depts.ddh.kiln.cocoon.transformation;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.avalon.framework.configuration.Configurable;
import org.apache.avalon.framework.configuration.Configuration;
import org.apache.avalon.framework.configuration.ConfigurationException;
import org.apache.avalon.framework.logger.Logger;
import org.apache.avalon.framework.parameters.ParameterException;
import org.apache.avalon.framework.parameters.Parameters;
import org.apache.cocoon.ProcessingException;
import org.apache.cocoon.environment.SourceResolver;
import org.apache.cocoon.transformation.AbstractDOMTransformer;
import org.eclipse.rdf4j.RDF4JException;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.rdfxml.RDFXMLWriter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

public class RDFTransformer extends AbstractDOMTransformer implements Configurable {

    public static final String BASE_URI_PARAM = "base-uri";
    public static final String BASE_URI_DEFAULT = null;
    public static final String RDF_SERVER_ACTION_PARAM = "action";
    public static final String RDF_SERVER_CONTEXTS_PARAM = "contexts";
    public static final String RDF_SERVER_CONTEXTS_DEFAULT = null;
    public static final String RDF_SERVER_REPOSITORY_ID_PARAM = "repository";
    public static final String RDF_SERVER_URL_PARAM = "url";
    public static final String RDF_SERVER_URL_DEFAULT = "http://localhost:9999/sesame/";

    private String action = null;
    private String baseURI = null;
    private RepositoryConnection connection = null;
    private Resource[] context;
    private String contexts = null;
    private Logger logger = null;
    private String rdfServerURL = null;


    @Override
    public void configure(Configuration conf) throws ConfigurationException {
        this.rdfServerURL = conf.getChild(RDF_SERVER_URL_PARAM).getValue(RDF_SERVER_URL_DEFAULT);
        this.baseURI = conf.getChild(BASE_URI_PARAM).getValue(BASE_URI_DEFAULT);
        this.contexts = conf.getChild(RDF_SERVER_CONTEXTS_PARAM).getValue(RDF_SERVER_CONTEXTS_DEFAULT);
    }

    /**
     * Sets up the transformer. Called when the pipeline is assembled. The
     * parameters are those specified as child elements of the
     * <code>&lt;map:transform&gt;</code> element in the sitemap.
     *
     * @see org.apache.cocoon.sitemap.SitemapModelComponent#setup(org.apache.cocoon.environment.SourceResolver,
     *      java.util.Map, java.lang.String,
     *      org.apache.avalon.framework.parameters.Parameters)
     */
    @SuppressWarnings("rawtypes")
    public void setup(SourceResolver resolver, Map objectModel, String src, Parameters parameters)
        throws ProcessingException, SAXException, IOException {
        this.logger = getLogger();
        try {
            // Get the server details from the parameters.
            this.rdfServerURL = parameters.getParameter(RDF_SERVER_URL_PARAM, this.rdfServerURL);
            String repositoryID = parameters.getParameter(RDF_SERVER_REPOSITORY_ID_PARAM);
            this.contexts = parameters.getParameter(RDF_SERVER_CONTEXTS_PARAM, this.contexts);
            this.action = parameters.getParameter(RDF_SERVER_ACTION_PARAM);
            HTTPRepository repository = new HTTPRepository(this.rdfServerURL, repositoryID);
            repository.initialize();
            ValueFactory factory = repository.getValueFactory();

            this.context = createContexts(this.contexts, factory);
            this.connection = repository.getConnection();
        } catch (TransformerFactoryConfigurationError e) {
            this.logger.error(e.getLocalizedMessage(), e);
            throw new ProcessingException(e.getMessage(), e);
        } catch (ParameterException e) {
            this.logger.error(e.getLocalizedMessage(), e);
            throw new ProcessingException(e.getMessage(), e);
        } catch (RepositoryException e) {
            this.logger.error(e.getLocalizedMessage(), e);
            throw new ProcessingException(e.getMessage(), e);
        }
    }

    /**
     * Returns an array of Resources derived from a space separated list
     * of contexts.
     *
     * @param contexts
     * @param factory
     * @return
     */
    private Resource[] createContexts(String contexts, ValueFactory factory) {
        /* The RDF4J API makes a distinction between passing a
         * (Resource)null and not passing anything as a context argument.
         * Therefore handle the string value "null" specially.
         *
         * See http://www.openrdf.org/doc/sesame2/users/ch08.html#d0e1238
         */
        Resource[] context;
        if (contexts == null) {
            context = null;
        } else {
            String[] arrContexts = this.contexts.split(" ");
            context = new Resource[arrContexts.length];
            for (int i = 0; i < arrContexts.length; i++) {
                this.logger.error("Context: '" + arrContexts[i] + "'");
                if (arrContexts[i].equals("null")) {
                    context[i] = null;
                } else {
                    context[i] = factory.createURI(arrContexts[i]);
                }
            }
        }
        return context;
    }

    /**
     * Perform an action against the Sesame server, returning the server's
     * response.
     *
     * This method delegates to other methods to do the work, based on the
     * action specified in the sitemap.
     *
     * @see org.apache.cocoon.transformation.AbstractDOMTransformer#transform(org.w3c.dom.Document)
     */
    protected Document transform(Document document) {

        // DOM Document containing the response from Sesame (or one generated
        // here in the absence of one from the server).
        Document responseDocument = null;

        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            responseDocument = builder.newDocument();
            Element root = responseDocument.createElement("response");
            responseDocument.appendChild(root);
            try {
                if (this.action.equals("add")) {
                    responseDocument = addDocument(document, this.baseURI, context, responseDocument);
                    Element success = responseDocument.createElement("success");
                    root.appendChild(success);
                }
                else if (this.action.equals("clear")) {
                    responseDocument = clearContext(context, responseDocument);
                    Element success = responseDocument.createElement("success");
                    root.appendChild(success);
                }
                else if (this.action.equals("graph-query")) {
                    responseDocument = performGraphQuery(document, this.baseURI);
                }
                else {
                    String errorMessage = "Invalid action parameter supplied: " + this.action; 
                    this.logger.error(errorMessage);
                    Element error = responseDocument.createElement("error");
                    error.setTextContent(errorMessage);
                    root.appendChild(error);
                }
            } catch (Exception e) {
                String errorMessage = e.getLocalizedMessage();
                this.logger.error(errorMessage, e);
                Element error = responseDocument.createElement("error");
                error.setTextContent(errorMessage);
                root.appendChild(error);
            }
        } catch (ParserConfigurationException e) {
            this.logger.error(e.getLocalizedMessage(), e);
        } catch (TransformerFactoryConfigurationError e) {
            this.logger.error(e.getLocalizedMessage(), e);
        } catch (RDF4JException e) {
            this.logger.error(e.getLocalizedMessage(), e);
        }
        return responseDocument;
    }

    /**
     * Add the statements from an RDF/XML document into the repository, in
     * the supplied contexts.
     *
     * @param document
     * @param baseURI
     * @param context
     * @param responseDocument
     * @return Document
     * @throws TransformerConfigurationException
     * @throws TransformerException
     * @throws TransformerFactoryConfigurationError
     * @throws RDFParseException
     * @throws RepositoryException
     * @throws IOException
     */
    private Document addDocument(Document document, String baseURI, Resource[] context, Document responseDocument)
        throws TransformerConfigurationException, TransformerException, TransformerFactoryConfigurationError, RDFParseException, RepositoryException, IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Source xmlSource = new DOMSource(document);
        Result outputTarget = new StreamResult(outputStream);
        TransformerFactory.newInstance().newTransformer().transform(xmlSource, outputTarget);
        InputStream is = new ByteArrayInputStream(outputStream.toByteArray());
        this.connection.add(is, baseURI, RDFFormat.RDFXML, context);
        return responseDocument;
    }

    /**
     * Remove all statements in the repository that belong to any of the supplied contexts.
     *
     * @param context
     * @param responseDocument
     * @return Document
     * @throws RepositoryException
     */
    private Document clearContext(Resource[] context, Document responseDocument)
        throws RepositoryException {
        if (context.length > 0) {
            this.connection.clear(context);
        } else {
            this.connection.clear();
        }
        return responseDocument;
    }

    /**
     * Perform a graph query taken from queryDocument, and return the RDF/XML result document.
     *
     * @param queryDocument
     * @param baseURI
     * @return Document
     * @throws QueryEvaluationException
     * @throws RepositoryException
     * @throws MalformedQueryException
     * @throws RDFHandlerException
     * @throws SAXException
     * @throws IOException
     * @throws ParserConfigurationException
     */
    private Document performGraphQuery(Document queryDocument, String baseURI)
        throws QueryEvaluationException, RepositoryException, MalformedQueryException, RDFHandlerException, SAXException, IOException, ParserConfigurationException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        RDFXMLWriter rdfXMLWriter = new RDFXMLWriter(outputStream);
        String query = queryDocument.getDocumentElement().getTextContent();
        this.connection.prepareGraphQuery(QueryLanguage.SPARQL, query, baseURI).evaluate(rdfXMLWriter);
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        Document responseDocument = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputStream);
        return responseDocument;
    }
}
