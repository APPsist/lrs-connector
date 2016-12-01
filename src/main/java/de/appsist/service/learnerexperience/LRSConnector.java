package de.appsist.service.learnerexperience;

import java.io.IOException;
import java.net.*;
import java.util.*;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.logging.Logger;
import org.vertx.java.core.logging.impl.LoggerFactory;
import org.vertx.java.platform.Verticle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rusticisoftware.tincan.*;
import com.rusticisoftware.tincan.json.StringOfJSON;
import com.rusticisoftware.tincan.lrsresponses.StatementLRSResponse;
import com.rusticisoftware.tincan.lrsresponses.StatementsResultLRSResponse;
import com.rusticisoftware.tincan.v10x.StatementsQuery;

import de.appsist.commons.misc.StatusSignalConfiguration;
import de.appsist.commons.misc.StatusSignalSender;


public class LRSConnector
    extends Verticle
{
    // JsonObject representing the configuration of this verticle
    private JsonObject config;
    // routematcher to match http queries
    private BasePathRouteMatcher routeMatcher = null;
    // verticle logger
    private static final Logger log = LoggerFactory.getLogger(LRSConnector.class);

    // remote learning record store
    private RemoteLRS lrs = null;
   
    // basePath String
    private String basePath = "";
    
    final static private String FALLBACK_HOMEPAGE = "http://dev.appsist.de";

    // ontology prefix
    private final String ontologyPrefix = "http://www.appsist.de/ontology/";

    // eventbus prefix
    private final String eventbusPrefix = "appsist:";

    // label storage map<URI String, Label String>
    private Map<String, String> labelStorage = null;

    // (de)-activate debugging
    private final static boolean isDebug = true;

    @Override
  public void start() {

        // ensure verticle is configured
        if (container.config() != null && container.config().size() > 0) {
            config = container.config();
        }
        else {
            // container.logger().warn("Warning: No configuration applied! Using default settings.");
            config = getDefaultConfiguration();
        }

        initializeLearningRecordStoreConnection();
        this.labelStorage = new HashMap<String, String>();

        this.basePath = config.getObject("webserver").getString("basePath");
        // init SparQL prefix string


        initializeHttpRequestHandlers();
        initializeEventbusHandlers();
        log.info("*******************");
        log.info("  LRSConnector auf Port "
                + config.getObject("webserver").getNumber("port") + " gestartet ");
        log.info("                              *******************");
        JsonObject statusSignalObject = config.getObject("statusSignal");
        StatusSignalConfiguration statusSignalConfig;
        if (statusSignalObject != null) {
          statusSignalConfig = new StatusSignalConfiguration(statusSignalObject);
        } else {
          statusSignalConfig = new StatusSignalConfiguration();
        }

        StatusSignalSender statusSignalSender =
          new StatusSignalSender("lrs-connector", vertx, statusSignalConfig);
        statusSignalSender.start();

  }


    private void initializeHttpRequestHandlers()
    {
        // init routematcher with basePath from configuration
        routeMatcher = new BasePathRouteMatcher(this.basePath);
        // set handlers here

        routeMatcher.get("/callTest", new Handler<HttpServerRequest>()
        {
            @Override
            public void handle(final HttpServerRequest request)
            {
                log.debug("Test method called");
                // testLRSStorage();
                request.response().end("done. Found " + testLRSStorage() + "entries");
            }
        });
        
        routeMatcher.post("/statements", new Handler<HttpServerRequest> ()
        {
            @Override
            public void handle(final HttpServerRequest request)
            {
                try {

                    Statement st = new Statement(
                            new StringOfJSON(request.params().get("statement")));
                    StatementLRSResponse lrsRes = lrs.saveStatement(st);
                    if (lrsRes.getSuccess()) {
                        // success, use lrsRes.getContent() to get the statement back
                        log.debug(lrsRes.getContent());
                    }
                    else {
                        // failure, error information is available in lrsRes.getErrMsg()
                        log.debug(lrsRes);
                        log.debug(lrsRes.getErrMsg());
                    }

                }
                catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                catch (URISyntaxException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
        
        // start verticle webserver at configured port
        vertx.createHttpServer().requestHandler(routeMatcher)
                .listen(config.getObject("webserver").getInteger("port"));

    }

    private void initializeEventbusHandlers()
    {

        Handler<Message<JsonObject>> storeStatementHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> message)
            {
                // retrieve statement from json object
                String statementString = message.body().getString("statement");
                // store statement
                storeLearningExperience(statementString);
            }
        };
        vertx.eventBus().registerHandler(this.eventbusPrefix + "query:lrs#storeStatement",
                storeStatementHandler);

        Handler<Message<JsonObject>> buildAndStoreStatementHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> message)
            {
                // retrieve statement from json object
                String agent = message.body().getString("agent");
                String verb = message.body().getString("verb");
                String object = message.body().getString("activityId");
                // store statement
                storeLearningExperience(agent, verb, object);
            }
        };

        vertx.eventBus().registerHandler(this.eventbusPrefix + "query:lrs#buildAndStoreStatement",
                buildAndStoreStatementHandler);

        Handler<Message<JsonObject>> numberOfStatementsHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> message)
            {
                // retrieve query from json object
                JsonObject body = message.body();
                String agent = body.getString("agent");
                String verb = body.getString("verb");
                String activityId = body.getString("activityId");
                if (activityId.startsWith("http://www.appsist.de/ontology/")) {
                    StatementsResult statementsResult = queryLearningExperience(agent, verb,
                            activityId);
                    sendAmount(statementsResult, message);
                }
                else {
                    findFullProcessIdAndSendAmount(message);
                }
            }
        };
        vertx.eventBus().registerHandler(this.eventbusPrefix + "query:lrs#getNumberOfStatements",
                numberOfStatementsHandler);
        
        Handler<Message<JsonObject>> interactedProductionItemsHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> message)
            {
                // retrieve query from json object
                JsonObject body = message.body();
                String agent = body.getString("agent");
                String verb = "http://adlnet.gov/expapi/verbs/interacted";
                StatementsResult interactedProductionItems = queryLearningExperience(agent, verb, null);
                log.info(interactedProductionItems.toJSON());
            }
        };
        vertx.eventBus().registerHandler(this.eventbusPrefix + "query:lrs#getInteractedProductionItems",
        		interactedProductionItemsHandler);

        Handler<Message<JsonObject>> postQueryHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> message)
            {
                // retrieve query from json object
                String agent = message.body().getString("agent");
                String verb = message.body().getString("verb");
                String object = message.body().getString("activityId");
                // get results
                // TODO: implement method
                // return results
            }
        };
        vertx.eventBus().registerHandler(this.eventbusPrefix + "query:lrs#postQuery",
                postQueryHandler);

        Handler<Message<JsonObject>> voidLearningProgressHandler = new Handler<Message<JsonObject>>()
        {
            public void handle(Message<JsonObject> message)
            {
                // retrieve query from json object
                String agent = message.body().getString("userId");
                // get results
                // TODO: implement method
                // return results
                voidLearningStatements(agent, "http://adlnet.gov/expapi/verbs/completed");
            }
        };
        vertx.eventBus().registerHandler(this.eventbusPrefix + "query:lrs#voidLearningProgress",
                voidLearningProgressHandler);

    }

    private void voidLearningStatements(String actor, String verb)
    {
        StatementsResult statementResult = queryLearningExperience(actor, verb, null);
        for (Statement statement : statementResult.getStatements()) {
            if (statement.getVerb().getId().toString().indexOf("voided") == -1) {
                if (!statement.getObject().getObjectType().equals("StatementRef")) {
                    String statementRefId = statement.getObject().toJSONNode(TCAPIVersion.V100)
                            .get("id").asText();
                    // log.debug("storeVoidStatement(\"cthulhu@appsistlrs.de\",\"" +
                    // statementRefId
                    // + "\")");
                    storeVoidStatement("cthulhu@appsistlrs.de", statement.getId().toString());
                }

            }
        }
    }

    // TODO: rename method
    private void findFullProcessIdAndSendAmount(final Message<JsonObject> message)
    {
        // expand processId to full ontology URI
        
        JsonObject request = new JsonObject();
        final String processId = message.body().getString("activityId", "");

        String sparqlQueryForProcessId = "PREFIX app: <http://www.appsist.de/ontology/> PREFIX terms: <http://purl.org/dc/terms/> "
                + " SELECT DISTINCT ?uri WHERE { ?uri a ?_ FILTER (REGEX(str(?uri),'" + processId
                + "$')) }";
        if (isDebug) {
            log.debug("[LRSConnector] sending SPARQL query: " + sparqlQueryForProcessId);
        }
        JsonObject sQuery = new JsonObject();
        sQuery.putString("query", sparqlQueryForProcessId);
        request.putObject("sparql", sQuery);

        vertx.eventBus().send(eventbusPrefix + "requests:semwiki", request,
                new Handler<Message<String>>()
                {
                    public void handle(Message<String> reply)
                    {
                        List<String> foundProcessIds = new ArrayList<String>();
                        try {
                            ObjectMapper mapper = new ObjectMapper();
                            JsonNode root = mapper.readTree(reply.body());
                            if (null != root) {
                                foundProcessIds = root.findValuesAsText("value");
                            }
                            if (!foundProcessIds.isEmpty()) {
                                String fullProcessId = foundProcessIds.get(0);
                                if (isDebug) {
                                    log.debug("[LRS-Connector] found fullProcessId: "
                                            + fullProcessId);
                                }
                                    // TODO: send LRS Request
                                    StatementsResult statementsResult = queryLearningExperience(
                                            message.body().getString("agent"),
                                            message.body().getString("verb"), fullProcessId);
                                    sendAmount(statementsResult, message);
                                }
                            else {
                                log.error("[LRS-Connector] no URI found in ontology for content: "
                                        + processId);
                            }
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }

                    };
                });
    }

    private void initializeLearningRecordStoreConnection()
    {
        try {
            this.lrs = new RemoteLRS();
            JsonObject lrsConfig = config.getObject("lrs");
            this.lrs.setEndpoint(lrsConfig.getString("endpoint"));
            this.lrs.setVersion(TCAPIVersion.V100);
            this.lrs.setUsername(lrsConfig.getString("username"));
            this.lrs.setPassword(lrsConfig.getString("password"));
            if (null != this.lrs) {
                log.debug("[LRS-connector] - Successful connected to LRS");
            }
            else {
                log.debug("[LRS-connector] - Connecting to LRS failed");
            }

        }
        catch (MalformedURLException e) {
            // tell service gateway service is not available
            // shutdown service
            log.error("MalformedURLException");
        }

    }
  

    /**
     * Create a configuration which is used if no configuration is passed to the module.
     * 
     * @return Configuration object.
     */
    private static JsonObject getDefaultConfiguration()
    {
        JsonObject defaultConfig = new JsonObject();
        JsonObject webserverConfig = new JsonObject();
        webserverConfig.putNumber("port", 8094);
        webserverConfig.putString("basePath", "/services/lrsconnect");
        // TODO: test statics with relative path
        // until now only full path is working

        defaultConfig.putObject("webserver", webserverConfig);

        JsonObject lrsConfig = new JsonObject();
        lrsConfig.putString("endpoint", "http://localhost:1234/data/xAPI");
        lrsConfig.putString("username", "f76aaf09f21be1f9e8bb1f634bc5b86db4a0603a");
        lrsConfig.putString("password", "7dc2379ee04bb6b064c98217aaff222c99aca798");
        defaultConfig.putObject("lrs", lrsConfig);
        defaultConfig.putString("homepage", "http://dev.appsist.de");
        return defaultConfig;
    }



    private void storeLearningExperience(String statement)
    {
        try {

            Statement st = new Statement(new StringOfJSON(statement));
            StatementLRSResponse lrsRes = lrs.saveStatement(st);
            if (lrsRes.getSuccess()) {
                // success, use lrsRes.getContent() to get the statement back
                log.debug("[LRS-connector] - Statement stored in LRS: " + lrsRes.getContent());

            }
            else {
                // failure, error information is available in lrsRes.getErrMsg()
                log.debug("[LRS-connector] - Storing statement in LRS " + "failed: "
                        + lrsRes.getErrMsg());

            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }



    private void storeLearningExperience(String actor, String verb, String activityId)
    {
        if (null == actor || null == verb || null == activityId) {
            // invalid statement
            return;
        }

        if (actor.equals("") || verb.equals("") || activityId.equals("")) {
            // invalid statement
            return;
        }

        Agent stAgent = null;
        Verb stVerb = null;
        Activity stActivity = null;
        // preparations for statement
        // create agent account
        AgentAccount aa = new AgentAccount();
        String stHomepage = config.getString("homepage");
        if (stHomepage == null) {
            stHomepage = FALLBACK_HOMEPAGE;
        }
        try {
            URL homepageURL = new URL(stHomepage);

        }
        catch (MalformedURLException mue) {
            stHomepage = FALLBACK_HOMEPAGE;
        }

        aa.setHomePage(stHomepage);
        aa.setName(actor);

        LanguageMap verbDisplays = new LanguageMap();
        String currentVerb = verb.substring(verb.lastIndexOf("/") + 1);
        verbDisplays.put("en-US", currentVerb);

        try {
            stAgent = new Agent();
            stAgent.setAccount(aa);
            stVerb = new Verb(verb);
            stVerb.setDisplay(verbDisplays);
            stActivity = new Activity();
            stActivity.setId(activityId);
        }
        catch (Exception e) {
            e.printStackTrace();
            // store failed statement parameters
        }
        Statement st = new Statement(stAgent, stVerb, stActivity);
        log.debug("[LRS-connector] - generated statement: " + st.toJSON());

        StatementLRSResponse lrsRes = this.lrs.saveStatement(st);
        if (null != lrsRes) {
            if (lrsRes.getSuccess()) {
                // success, use lrsRes.getContent() to get the statement back
                log.debug("[LRS-connector] - Statement stored in LRS: " + lrsRes.getContent());
            }
            else {
                // failure, error information is available in lrsRes.getErrMsg()
                log.debug(
                        "[LRS-connector] - Storing statement in LRS failed: " + lrsRes.getErrMsg());
            }
        }
        else {
            log.debug("[LRS-connector] - Storing statement in LRS failed: LRS Response is null");
        }

    }

    private void storeVoidStatement(String actor, String statementId)
    {
        if (null == actor) {
            // invalid statement
            return;
        }

        if (actor.equals("")) {
            // invalid statement
            return;
        }

        Agent stAgent = null;
        Verb stVerb = null;
        StatementRef stStatementRef = null;
        // preparations for statement
        // create agent account
        AgentAccount aa = new AgentAccount();
        String stHomepage = config.getString("homepage");
        if (stHomepage == null) {
            stHomepage = FALLBACK_HOMEPAGE;
        }
        try {
            URL homepageURL = new URL(stHomepage);

        }
        catch (MalformedURLException mue) {
            stHomepage = FALLBACK_HOMEPAGE;
        }

        aa.setHomePage(stHomepage);
        aa.setName(actor);

        LanguageMap verbDisplays = new LanguageMap();
        verbDisplays.put("en-US", "voided");

        try {
            stAgent = new Agent();
            stAgent.setAccount(aa);
            stVerb = new Verb("http://adlnet.gov/expapi/verbs/voided");
            stVerb.setDisplay(verbDisplays);
            stStatementRef = new StatementRef(UUID.fromString(statementId));
        }
        catch (Exception e) {
            e.printStackTrace();
            // store failed statement parameters
        }
        Statement st = new Statement(stAgent, stVerb, stStatementRef);

        // log.debug("[lrs] - timestamp: " + st.getTimestamp().toLocalDateTime());

        StatementLRSResponse lrsRes = this.lrs.saveStatement(st);
        if (null != lrsRes) {
            if (lrsRes.getSuccess()) {
                // success, use lrsRes.getContent() to get the statement back
                log.debug("[LRS-connector] - Statement stored in LRS: " + lrsRes.getContent());
            }
            else {
                // failure, error information should be available in lrsRes.getErrMsg()
                log.error(
                        "[LRS-connector] - Storing statement in LRS failed: " + lrsRes.getErrMsg());
            }
        }
        else {
            log.error("[LRS-connector] - Storing statement in LRS failed: LRS Response is null");
        }
    }

    private StatementsResult queryLearningExperience(String actor, String verb, String activityId)
    {
        log.debug("[LRS-Connector] - building LRS query with params: " + actor + "|" + verb + "|"
                + activityId);

        // if (actor.equals("") || verb.equals("") || activityId.equals("")) {
        if (actor.equals("") || verb.equals("")) {
            // invalid statement
            return new StatementsResult();
        }

        Agent stAgent = null;
        Verb stVerb = null;
        URI activityURI = null;

        AgentAccount aa = new AgentAccount();
        String stHomepage = config.getString("homepage");
        if (stHomepage == null) {
            stHomepage = FALLBACK_HOMEPAGE;
        }
        try {
            URL homepageURL = new URL(stHomepage);

        }
        catch (MalformedURLException mue) {
            stHomepage = FALLBACK_HOMEPAGE;
        }

        aa.setHomePage(stHomepage);
        aa.setName(actor);

        LanguageMap verbDisplays = new LanguageMap();
        String currentVerb = verb.substring(verb.lastIndexOf("/"));
        verbDisplays.put("en-US", currentVerb);
        try {
            stAgent = new Agent();
            // stAgent.setMbox(mBoxString);
            stAgent.setAccount(aa);
            stVerb = new Verb(verb);
            if (null != activityId) {
                activityURI = new URI(activityId);
            }

            StatementsQuery sq = new StatementsQuery();
            sq.setAgent(stAgent);
            sq.setVerbID(stVerb);
            sq.setLimit(1000);
            if (null != activityURI) {
                sq.setActivityID(activityURI);
            }

            StatementsResultLRSResponse lrsRes = lrs.queryStatements(sq);

            if (lrsRes.getSuccess()) {
                // success, use lrsRes.getContent() to get the statement back
                if (isDebug) {
                    log.debug(
                            "[LRS-Connectorc] - found " + lrsRes.getContent().getStatements().size()
                                    + " matching statements for query: " + sq.toString());
                }

                return lrsRes.getContent();
            }
            else {
                // failure, error information is available in lrsRes.getErrMsg()
                log.error(lrsRes.getErrMsg() + " for query " + sq.toString());
                return new StatementsResult();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            return new StatementsResult();
            // store failed statement parameters
        }
    }

    private void sendAmount(StatementsResult statementsResult, Message<JsonObject> message)
    {
        JsonObject body = message.body();
        if (null != statementsResult) {
            int statementCount = 0;
            for (Statement st : statementsResult.getStatements()) {
                if (st.getVerb().getId().toString().indexOf("voided") == -1) {
                    statementCount++;
                }
            }
            body.putNumber("amount", statementCount);
        }
        else {
            body.putNumber("amount", -1);
        }

        message.reply(body);
    }

    private int testLRSStorage()
    {
        String actor = "daniel.novize@appsist.de";
        String verb = "http://adlnet.gov/expapi/verbs/interacted";
        String activityId = "http://www.appsist.de/ontology/festo/0b4e2ad2-09dc-11e5-a6c0-1697f925ec7b";
        StatementsResult statementResult = queryLearningExperience(actor, verb, null);

        log.debug("[LRS-Connector] - statementResult " + statementResult.toJSON(true));
        // String statement = "{\"actor\": {\"account\": {\"homePage\":
        // \"http://www.example.com\",\"name\": \"1625378\"}, \"objectType\": \"Agent\"},
        // \"verb\": {\"id\": \"http://adlnet.gov/expapi/verbs/failed\", \"display\":
        // {\"en-US\": \"failed\"}},\"object\": {\"id\":
        // \"http://www.example.com/tincan/activities/RtYaEwGM\",\"objectType\":
        // \"Activity\",\"definition\": {\"name\": {\"en-US\": \"Example
        // Activity\"},\"description\": {\"en-US\": \"Example activity definition\"}}}}";
        // String statement =
        // "{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://dev.appsist.de\",\"name\":\"cthulhu\"}},\"verb\":{\"id\":\"http://adlnet.gov/expapi/verbs/voided\",\"display\":{\"en-US\":\"voided\"}},\"object\":{\"objectType\":\"StatementRef\",\"id\":\"56d3f021-7a4d-4a3e-ad14-508990548359\"}}";

        // storeLearningExperience(statement);
        return statementResult.getStatements().size();
    }
}
