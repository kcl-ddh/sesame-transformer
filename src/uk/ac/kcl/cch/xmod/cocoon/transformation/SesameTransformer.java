/**
 * SesameTransformer.java
 * Cocoon transformer to interact with a Sesame 2 server. 
 */

package uk.ac.kcl.cch.xmod.cocoon.transformation;

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
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.http.HTTPRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

/**
 * This class receives an RDF/XML document, sends it to a Sesame server, and
 * returns the server's response.
 * 
 * @author Jamie Norrish jamie@artefact.org.nz
 * @author jvieira jose.m.vieira@kcl.ac.uk
 */
public class SesameTransformer extends AbstractDOMTransformer implements Configurable {

	/**
	 * Sesame server URL parameter name.
	 */
	public static final String SESAME_URL_PARAM = "url";
	
    /**
     * Sesame server default URL.
     */
    public static final String SESAME_URL_DEFAULT = "http://localhost:9999/sesame/";

    /**
     * Sesame server URL.
     */
    private String sesameURL = null;
    
    /**
     * Sesame repository ID parameter name.
     */
    public static final String SESAME_REPOSITORY_ID_PARAM = "repository";
    
    /**
     * Sesame repository ID.
     */
    private String repositoryID = null;
    
    /**
     * Base URI parameter name.
     */
    public static final String BASE_URI_PARAM = "base_uri";
    
    /**
     * Default base URI.
     */
    public static final String BASE_URI_DEFAULT = null;
    
    /**
     * Base URI.
     */
    private String baseURI = null;

    /**
     * Sesame contexts parameter name.
     */
    public static final String SESAME_CONTEXTS_PARAM = "contexts";
    
    /**
     * Sesame default contexts.
     */
    public static final String SESAME_CONTEXTS_DEFAULT = "";
    
    /**
     * Sesame contexts (space separated list).
     */
    private String contexts = null;
    
    /**
     * Sesame contexts (as used by Sesame).
     */
    private Resource[] context;

    /**
     * Sesame action parameter name.
     */
    public static final String SESAME_ACTION_PARAM = "action";
    
    /**
     * Sesame action. This may be one of: "add", "clear".
     */
    private String action = null;
    
    /**
     * Connection to a repository on the Sesame server.
     */
    private RepositoryConnection sesameConnection = null;
    
    /**
     * Reference to the Cocoon logger.
     */
    private Logger logger = null;

    /**
     * Configures the transformer.
     * 
     * @see org.apache.avalon.framework.configuration.Configurable#configure(org.apache.avalon.framework.configuration.Configuration)
     */
	public void configure(Configuration conf) throws ConfigurationException {
		
		this.sesameURL = conf.getChild(SESAME_URL_PARAM).getValue(SESAME_URL_DEFAULT);
		this.baseURI = conf.getChild(BASE_URI_PARAM).getValue(BASE_URI_DEFAULT);
		this.contexts = conf.getChild(SESAME_CONTEXTS_PARAM).getValue(SESAME_CONTEXTS_DEFAULT);

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
            // Get the Sesame details from the parameters.
            this.sesameURL = parameters.getParameter(SESAME_URL_PARAM, sesameURL);
            this.repositoryID = parameters.getParameter(SESAME_REPOSITORY_ID_PARAM);
            this.contexts = parameters.getParameter(SESAME_CONTEXTS_PARAM, contexts);
            this.action = parameters.getParameter(SESAME_ACTION_PARAM);
            logger.error("Contexts: " + this.contexts);
            Repository repository = new HTTPRepository(this.sesameURL, this.repositoryID);
            repository.initialize();
            ValueFactory factory = repository.getValueFactory();

            this.context = createContexts(this.contexts, factory);
            this.sesameConnection = repository.getConnection();
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
    	/* The Sesame API makes a distinction between passing a
    	 * (Resource)null and not passing anything as a context argument.
    	 * Therefore handle the string value "null" specially.
    	 *  
    	 * See http://www.openrdf.org/doc/sesame2/users/ch08.html#d0e1238  
    	 */
        String[] arrContexts = this.contexts.split(" ");
        Resource[] context = new Resource[arrContexts.length];
        for (int i = 0; i < arrContexts.length; i++) {
        	if (arrContexts[i].equals("null")) {
        		context[i] = null;
        	} else {
        		context[i] = factory.createURI(arrContexts[i]);
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

			if (this.action.equals("add")) {
				responseDocument = addDocument(document, this.baseURI, context, responseDocument);
			}
			else if (this.action.equals("clear")) {
				responseDocument = clearContext(context, responseDocument);
			}
			else {
				this.logger.error("Invalid action parameter supplied: " + this.action);
			}
		} catch (ParserConfigurationException e) {
			this.logger.error(e.getLocalizedMessage(), e);
		} catch (RepositoryException e) {
			this.logger.error(e.getLocalizedMessage(), e);
		} catch (TransformerConfigurationException e) {
			this.logger.error(e.getLocalizedMessage(), e);
		} catch (RDFParseException e) {
			this.logger.error(e.getLocalizedMessage(), e);
		} catch (TransformerException e) {
			this.logger.error(e.getLocalizedMessage(), e);
		} catch (TransformerFactoryConfigurationError e) {
			this.logger.error(e.getLocalizedMessage(), e);
		} catch (IOException e) {
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
	 * @return
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
		this.sesameConnection.add(is, baseURI, RDFFormat.RDFXML, context);
		return responseDocument;
		
	}

	/**
	 * Remove all statements in the repository that belong to any of the supplied contexts.
	 * 
	 * @param context
	 * @param responseDocument
	 * @return
	 * @throws RepositoryException 
	 */
	private Document clearContext(Resource[] context, Document responseDocument)
			throws RepositoryException {

		if (context.length > 0) {
			this.sesameConnection.clear(context);
		} else {
			this.sesameConnection.clear();
		}
		return responseDocument;
	
	}

}
