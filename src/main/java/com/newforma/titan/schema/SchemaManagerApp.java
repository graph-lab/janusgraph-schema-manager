package com.newforma.titan.schema;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newforma.titan.schema.actions.ReindexAction;
import com.newforma.titan.schema.actions.ReindexAction.IndexTarget;
import com.newforma.titan.schema.actions.ReindexAction.IndexingMethod;

public class SchemaManagerApp {

    private static final String OPTION_GRAPH_CONFIG_FILE = "g";
    private static final String OPTION_GRAPH_CONFIG_FILE_LONG = "graph-config";
    private static final String OPTION_GENERATE_DOCS = "d";
    private static final String OPTION_GENERATE_DOCS_LONG = "generate-documentation";
    private static final String OPTION_REINDEX_SPECIFIC = "i";
    private static final String OPTION_REINDEX_SPECIFIC_LONG = "index-name";
    private static final String OPTION_REINDEX_DATA = "r";
    private static final String OPTION_REINDEX_DATA_LONG = "reindex-data";
    private static final String OPTION_WRITE_TO_DB = "w";
    private static final String OPTION_WRITE_TO_DB_LONG = "write";
    private static final String OPTION_LOAD_GRAPHML = "l";
    private static final String OPTION_LOAD_GRAPHML_LONG = "load-data";
    private static final String OPTION_SAVE_GRAPHML = "s";
    private static final String OPTION_SAVE_GRAPHML_LONG = "save-data";
    private static final String OPTION_FILTER_TAGS = "t";
    private static final String OPTION_FILTER_TAGS_LONG = "tag-filter";
    private static final String OPTION_INDEXING_METHOD = "m";
    private static final String OPTION_INDEXING_METHOD_LONG = "index-method";
    private static final String OPTION_REINDEX_TIMEOUT = "it";
    private static final String OPTION_REINDEX_TIMEOUT_LONG = "index-wait-timeout";

    private static final Logger LOG = LoggerFactory.getLogger(SchemaManagerApp.class);

    public static void main(String[] args) {
        final Options options = populateOptions();
        final CommandLine cmdLine;
        try {
            cmdLine = new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.out.println("Invalid command: " + e.getMessage());
            printHelp(options);
            System.exit(1);
            return;
        }

        final String[] remainingArgs = cmdLine.getArgs();
        if (remainingArgs.length != 1) {
            System.out.println("Missing schema file name");
            printHelp(options);
            System.exit(1);
            return;
        }

        final boolean doApplyChanges = cmdLine.hasOption(OPTION_WRITE_TO_DB);
        final String graphConfigFile = cmdLine.getOptionValue(OPTION_GRAPH_CONFIG_FILE);
        final List<ReindexAction> reindexActions = new LinkedList<>();
        final IndexingMethod indexingMethod;
        if (cmdLine.hasOption(OPTION_INDEXING_METHOD)) {
            indexingMethod = IndexingMethod.valueOf(cmdLine.getOptionValue(OPTION_INDEXING_METHOD).toUpperCase());
        } else {
            // default to local
            indexingMethod = IndexingMethod.LOCAL;
        }
        if (cmdLine.hasOption(OPTION_REINDEX_DATA)) {
            reindexActions.add(new ReindexAction(IndexTarget.valueOf(cmdLine.getOptionValue(OPTION_REINDEX_DATA))));
        }
        if (cmdLine.hasOption(OPTION_REINDEX_SPECIFIC)) {
            reindexActions.add(new ReindexAction(IndexTarget.NAMED, indexingMethod, cmdLine.getOptionValue(OPTION_REINDEX_SPECIFIC)));
        }

        int reindexTimeoutInSecs = SchemaManager.DEFAULT_INDEX_REGISTERED_TIMEOUT_SECS;
        if (cmdLine.hasOption(OPTION_REINDEX_TIMEOUT)) {
        	reindexTimeoutInSecs = Integer.parseInt(cmdLine.getOptionValue(OPTION_REINDEX_TIMEOUT));
        }

        final String docDir = cmdLine.getOptionValue(OPTION_GENERATE_DOCS);
        final String graphMLToLoad = cmdLine.getOptionValue(OPTION_LOAD_GRAPHML);
        final String graphMLToSave = cmdLine.getOptionValue(OPTION_SAVE_GRAPHML);
        final String tagFilter = cmdLine.getOptionValue(OPTION_FILTER_TAGS);

        try {
            new SchemaManager(remainingArgs[0], graphConfigFile)
                    .andApplyChanges(doApplyChanges)
                    .andReindex(reindexActions)
                    .applyTagFilter(tagFilter).andGenerateDocumentation(docDir)
                    .andLoadData(graphMLToLoad)
                    .reindexingTimeout(reindexTimeoutInSecs)
                    .andSaveData(graphMLToSave).run();
        } catch (Throwable t) {
            LOG.error("ERROR", t);
            System.exit(1);
        }

        System.exit(0);
    }

    private static Options populateOptions() {
        final Options options = new Options();
        options.addOption(OPTION_WRITE_TO_DB, OPTION_WRITE_TO_DB_LONG, false, "Write the relations defined by the schema to the graph");
        options.addOption(OPTION_REINDEX_DATA, OPTION_REINDEX_DATA_LONG, true, "Reindex data: ALL, NEW, UNAVAILABLE");
        options.addOption(OPTION_REINDEX_SPECIFIC, OPTION_REINDEX_SPECIFIC_LONG, true, "Reindex the specific index after applying the schema");
        options.addOption(OPTION_INDEXING_METHOD, OPTION_INDEXING_METHOD_LONG, true, "Using the specific indexing method: one of " +
                StringUtils.join(ReindexAction.IndexingMethod.values(), ',') + " ("  +
                ReindexAction.IndexingMethod.LOCAL + " is the default)");
        options.addOption(OPTION_GENERATE_DOCS, OPTION_GENERATE_DOCS_LONG, true, "Generate documentation, write to the specified directory");
        options.addOption(OPTION_LOAD_GRAPHML, OPTION_LOAD_GRAPHML_LONG, true, "Load specific GraphML file into the database");
        options.addOption(OPTION_SAVE_GRAPHML, OPTION_SAVE_GRAPHML_LONG, true, "Save specific GraphML file into a file");
        options.addOption(OPTION_FILTER_TAGS, OPTION_FILTER_TAGS_LONG, true, "Apply tag filter for generated documentation. "
                + "Filter format: tag-spec[,tag-spec[,...]]. tag-spec ::= [!]tag-name[:tag-color]. "
                + "Colors are used for DOT diagram. If the filter is specified, then only the elements having the "
                + "specified tags will be included in the documentation and the elements having the tags prefixed with "
                + "\"!\" will be excluded.");
        options.addRequiredOption(OPTION_GRAPH_CONFIG_FILE, OPTION_GRAPH_CONFIG_FILE_LONG, true, "Graph property file name");
        options.addOption(OPTION_REINDEX_TIMEOUT, OPTION_REINDEX_TIMEOUT_LONG, true, "Specify the amount of time in seconds to wait before timing out on an index creation. Default 300 seconds.");
        return options;
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("SchemaManager options schema-file", options);
    }
}
