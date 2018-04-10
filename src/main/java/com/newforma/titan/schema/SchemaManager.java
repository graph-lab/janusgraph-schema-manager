package com.newforma.titan.schema;

import java.io.File;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newforma.titan.schema.GraphState.ElementType;
import com.newforma.titan.schema.actions.ReindexAction;
import com.newforma.titan.schema.actions.ReindexAction.IndexingMethod;
import com.newforma.titan.schema.types.GraphIndexDef;
import com.newforma.titan.schema.types.GraphIndexDef.IndexType;
import com.newforma.titan.schema.types.GraphIndexDef.RelType;
import com.newforma.titan.schema.types.GraphIndexKeyDef;
import com.newforma.titan.schema.types.GraphIndexingDefaults;
import com.newforma.titan.schema.types.GraphSchemaDef;
import com.newforma.titan.schema.types.LocalEdgeIndexDef;
import com.newforma.titan.schema.types.LocalPropertyIndexDef;
import com.newforma.titan.schema.types.SchemaEdgeLabel;
import com.newforma.titan.schema.types.SchemaPropertyDef;
import com.newforma.titan.schema.types.SchemaSortKey;
import com.newforma.titan.schema.types.SchemaVertexLabel;
import com.newforma.titan.schema.validator.SchemaValidationException;
import com.newforma.titan.schema.validator.SchemaValidator;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.EdgeLabelMaker;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.core.schema.RelationTypeIndex;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.JanusGraphManagement.IndexBuilder;
import org.janusgraph.core.schema.JanusGraphManagement.IndexJobFuture;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.core.schema.VertexLabelMaker;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.management.GraphIndexStatusReport;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.database.management.RelationIndexStatusReport;
import org.janusgraph.graphdb.types.StandardRelationTypeMaker;
import org.janusgraph.hadoop.MapReduceIndexManagement;

public class SchemaManager {

	private static final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);

	public static final int DEFAULT_INDEX_REGISTERED_TIMEOUT_SECS = 300;
	private final String graphSchemaFileName;
	private final String graphConfigFileName;
	private boolean doApplyChanges;
	private String docDir;
	private String graphMLFileToLoad;
	private String docTagFilter;
	private String graphMLFileToSave;
	private int reindexTimeoutInSecs = DEFAULT_INDEX_REGISTERED_TIMEOUT_SECS;

	private List<ReindexAction> reindexActions = Collections.emptyList();

	SchemaManager(final String graphSchemaFileName, final String graphConfigFileName) {
		this.graphSchemaFileName = graphSchemaFileName;
		this.graphConfigFileName = graphConfigFileName;
	}

	public SchemaManager andReindex(List<ReindexAction> reindexActions) {
		this.reindexActions = reindexActions;
		return this;
	}

	public SchemaManager andApplyChanges(boolean doApplyChanges) {
		this.doApplyChanges = doApplyChanges;
		return this;
	}

	public SchemaManager andGenerateDocumentation(String docDir) {
		this.docDir = docDir;
		return this;
	}

	public SchemaManager andLoadData(String graphMLFileToLoad) {
		this.graphMLFileToLoad = graphMLFileToLoad;
		return this;
	}

	public SchemaManager applyTagFilter(String docTagFiler) {
	    this.docTagFilter = docTagFiler;
	    return this;
	}

	public SchemaManager andSaveData(String graphMLFileToSave) {
		this.graphMLFileToSave = graphMLFileToSave;
		return this;
	}

	public SchemaManager reindexingTimeout(int timeoutInSecs) {
		this.reindexTimeoutInSecs = timeoutInSecs;
		return this;
	}

	void run() throws SchemaManagementException {
		LOG.debug("Processing schema from {} for graph {}, applying changes={}", graphSchemaFileName, graphConfigFileName,
				Boolean.valueOf(doApplyChanges));
		final GraphSchemaDef graphDef;
		try {
			graphDef = SchemaLoader.getInstance().loadFrom(new File(graphSchemaFileName));
		} catch (IOException e) {
			throw new SchemaManagementException("Failed to load the graph schema", e);
		} catch (SchemaValidationException e) {
			throw new SchemaManagementException("Failed to validate the graph schema", e);
		}

		LOG.debug("Successfully loaded graph schema: {}", graphDef);

		final PropertiesConfiguration graphConfig = new PropertiesConfiguration();
		final GraphState graphState;
		try {
			graphState = new GraphState(graphDef);
		} catch (SchemaValidationException e) {
			throw new SchemaManagementException("Graph schema inconsistency detected", e);
		}

		LOG.info("Connecting to the graph using {}", graphConfigFileName);
		try {
			graphConfig.load(new File(graphConfigFileName));
		} catch (ConfigurationException e) {
			throw new SchemaManagementException("Failed to load graph configuration from " + graphConfigFileName, e);
		}

		final JanusGraph graph = JanusGraphFactory.open(graphConfig);

		try {

			LOG.info("Graph connection successful");

			if (graph instanceof StandardJanusGraph
					&& ((StandardJanusGraph) graph).getBackend().getStoreFeatures().hasCellTTL()) {
				graphState.setTtlSupported(true);
			}

			// 1. Validate the values in the schema as much as possible

			LOG.debug("Validating graph schema definition");
			try {
				new SchemaValidator().validate(graphDef);
			} catch (SchemaValidationException e) {
				throw new SchemaManagementException("Failed to validate the graph schema", e);
			}

			// 2. For each schema element check if it exists in the database
			// already
			// and if it conflicts the definition
			LOG.debug("Verifying existing graph elements");
			verifyExistingGraphElements(graph, graphState, graphDef);

			// 3. For each non-existing relation type - create one (unless doing
			// dry-run)
			if (doApplyChanges) {
				populateNewGraphElements(graph, graphState, graphDef);
			} else {
				LOG.info("Dry-run: NOT creating graph elements");
			}

			reindexData(graph, graphState, reindexActions);

			if (!StringUtils.isEmpty(graphMLFileToLoad)) {
				new GraphMLLoader(graph, graphMLFileToLoad).run();
			}

			if (!StringUtils.isEmpty(graphMLFileToSave)) {
				new GraphMLSaver(graph, graphMLFileToSave).run();
			}

			if (!StringUtils.isEmpty(docDir)) {
				try {
					DocTagGraphFilter filter = new DocTagGraphFilter();
					final GraphState filteredGraphState = filter.filterSchema(graphState, docTagFilter);
					new DocGenerator().generate(filteredGraphState, docDir);
					new DOTGenerator().generate(filteredGraphState, docDir);
				} catch (IOException e) {
					throw new SchemaManagementException("Failed to generate the documentation", e);
				} catch (SchemaValidationException e) {
					throw new SchemaManagementException("Failed to validate the graph after applying the tag filters",
							e);
				}
			}
		} finally {
			if (graph != null) {
				graph.close();
			}
		}
	}

	private void reindexData(JanusGraph graph, GraphState graphState, List<ReindexAction> reindexActionList) throws SchemaManagementException {
		for(final ReindexAction action: reindexActionList) {
			switch(action.getTarget()) {
			case NAMED:
				updateSingleIndex(graphState, graph, action.getIndexName(), action.getMethod());
				break;
			case ALL:
				for(final String indexName: graphState.getAllIndexes()) {
					updateSingleIndex(graphState, graph, indexName, action.getMethod());
				}
				break;
			case NEW:
				for(final String indexName: graphState.getNewIndexes()) {
					updateSingleIndex(graphState, graph, indexName, action.getMethod());
				}
				break;
            case UNAVAILABLE:
                for(final String indexName: IndexUtils.getUnavailableIndexes(graphState.getAllIndexes(), graphState, graph)) {
                    LOG.info("Index {} is not available, updating", indexName);
                    updateSingleIndex(graphState, graph, indexName, action.getMethod());
                }
                break;
			default:
				throw new RuntimeException("Unsupported value " + action.getTarget());
			}
		}
	}

	private void updateSingleIndex(GraphState graphState, JanusGraph graph, String indexName, IndexingMethod indexingMethod) throws SchemaManagementException {
		graph.tx().rollback();

		Object indexDef = graphState.getIndexDef(indexName);

		if (indexDef == null) {
		    throw new SchemaManagementException("Unknown index " + indexName);
		}

		try {
			if (indexDef instanceof GraphIndexDef) {
				LOG.info("Updating graph index {}", indexName);
				ensureGraphIndexReady(graph, indexName);

				final JanusGraphManagement mgmtUp = graph.openManagement();
                switch (indexingMethod) {
                case LOCAL:
                    mgmtUp.updateIndex(mgmtUp.getGraphIndex(indexName), SchemaAction.REINDEX).get();
                    break;
                case HADOOP:
                case HADOOP2:
                    MapReduceIndexManagement mr = new MapReduceIndexManagement(graph);
                    mr.updateIndex(mgmtUp.getGraphIndex(indexName), SchemaAction.REINDEX).get();
                    break;
                default:
                    throw new RuntimeException("Unsupported reindexing method: " + indexingMethod);
                }

				mgmtUp.commit();
			} else if (indexDef instanceof LocalEdgeIndexDef) {
				LOG.info("Updating local edge index {}", indexName);
				LocalEdgeIndexDef localIndexDef = (LocalEdgeIndexDef)indexDef;
				final JanusGraphManagement mgmt = graph.openManagement();
				ensureLocalIndexReady(graph, mgmt.getRelationIndex(mgmt.getEdgeLabel(localIndexDef.getLabel()), indexName), localIndexDef.getLabel());
				mgmt.rollback();

				final JanusGraphManagement mgmtUp = graph.openManagement();
                switch (indexingMethod) {
                case LOCAL:
                    mgmtUp.updateIndex(mgmtUp.getRelationIndex(mgmtUp.getEdgeLabel(localIndexDef.getLabel()), indexName), SchemaAction.REINDEX).get();
                    break;
                case HADOOP:
                case HADOOP2:
                    MapReduceIndexManagement mr = new MapReduceIndexManagement(graph);
                    mr.updateIndex(mgmtUp.getRelationIndex(mgmtUp.getEdgeLabel(localIndexDef.getLabel()), indexName), SchemaAction.REINDEX).get();
                    break;
                default:
                    throw new RuntimeException("Unsupported reindexing method: " + indexingMethod);
                }
				mgmtUp.commit();
			} else if (indexDef instanceof LocalPropertyIndexDef) {
				LOG.info("Updating local property index {}", indexName);

				LocalPropertyIndexDef localIndexDef = (LocalPropertyIndexDef)indexDef;
				final JanusGraphManagement mgmt = graph.openManagement();
				ensureLocalIndexReady(graph, mgmt.getRelationIndex(mgmt.getPropertyKey(localIndexDef.getKey()), indexName), localIndexDef.getKey());
				mgmt.rollback();

				final JanusGraphManagement mgmtUp = graph.openManagement();
                switch (indexingMethod) {
                case LOCAL:
                    mgmtUp.updateIndex(mgmtUp.getRelationIndex(mgmtUp.getPropertyKey(localIndexDef.getKey()), indexName), SchemaAction.REINDEX).get();
                    break;
                case HADOOP:
                case HADOOP2:
                    MapReduceIndexManagement mr = new MapReduceIndexManagement(graph);
                    mr.updateIndex(mgmtUp.getRelationIndex(mgmtUp.getPropertyKey(localIndexDef.getKey()), indexName), SchemaAction.REINDEX).get();
                    break;
                default:
                    throw new RuntimeException("Unsupported reindexing method: " + indexingMethod);
                }
				mgmtUp.commit();
			} else {
				throw new SchemaManagementException("Unsupported index type " + indexDef);
			}
		} catch (InterruptedException | ExecutionException e) {
			throw new SchemaManagementException("Unable to update index", e);
		} catch (BackendException e) {
		    throw new SchemaManagementException("Backend error, unable to update index", e);
        }
	}

	private RelationTypeIndex ensureLocalIndexReady(JanusGraph graph, RelationTypeIndex index, String relationTypeName) throws SchemaManagementException {
		RelationTypeIndex index2 = ensureLocalIndexState(graph, index, relationTypeName, SchemaStatus.INSTALLED,
				SchemaAction.REGISTER_INDEX, SchemaStatus.REGISTERED);
		return ensureLocalIndexState(graph, index2, relationTypeName, SchemaStatus.REGISTERED,
				SchemaAction.ENABLE_INDEX, SchemaStatus.ENABLED);
	}


	private RelationTypeIndex ensureLocalIndexState(JanusGraph graph, RelationTypeIndex index, String relationTypeName, SchemaStatus testState,
			SchemaAction action, SchemaStatus targetState) throws SchemaManagementException {

		final JanusGraphManagement mgmt = graph.openManagement();
		RelationTypeIndex dbIndex = mgmt.getRelationIndex(index.getType(), index.name());

		final SchemaStatus oldState = dbIndex.getIndexStatus();

		if (oldState == testState) {
			LOG.warn("Index \"{}\" status is {}, attempting to {} it...", index.name(), testState, action);
			try {
				mgmt.updateIndex(dbIndex, action).get();	// useless - this future is not waiting for anything
				mgmt.commit();
				ManagementSystem.awaitRelationIndexStatus(graph, index.name(), relationTypeName)
					.status(targetState)
					.timeout(this.reindexTimeoutInSecs, ChronoUnit.SECONDS)
					.call();
				if (index.getIndexStatus() == oldState) {
					throw new SchemaManagementException("Unable to change index \"" + index.name() + "\" state using action " + action +
							", index is still in state " + oldState);
				}
				LOG.info("Index \"{}\" status has changed to {}", index.name(), index.getIndexStatus());
			} catch (Exception e) {
				throw new SchemaManagementException("Unable to change index \"" + index.name() + "\" state using action " + action, e);
			}
		} else {
			mgmt.rollback();
		}

		return index;
	}

    void ensureGraphIndexReady(JanusGraph graph, String graphIndexName) throws SchemaManagementException {
		ensureGraphIndexState(graph, graphIndexName, SchemaStatus.INSTALLED,
				SchemaAction.REGISTER_INDEX, SchemaStatus.REGISTERED);
		ensureGraphIndexState(graph, graphIndexName, SchemaStatus.REGISTERED,
				SchemaAction.ENABLE_INDEX, SchemaStatus.ENABLED);
	}

	private void ensureGraphIndexState(JanusGraph graph, String graphIndexName,
			SchemaStatus testState, SchemaAction action, SchemaStatus targetState) throws SchemaManagementException {

		JanusGraphManagement mgmt = graph.openManagement();
		final List<String> indexPKNames = Arrays.asList(mgmt.getGraphIndex(graphIndexName).getFieldKeys()).stream().map(PropertyKey::name).collect(Collectors.toList());
		mgmt.rollback();

		for(final String pkName: indexPKNames) {
		    mgmt = graph.openManagement();
			final SchemaStatus oldState = mgmt.getGraphIndex(graphIndexName).getIndexStatus(mgmt.getPropertyKey(pkName));
			if (oldState == testState) {
				LOG.warn("Index \"{}\" status is {} for property \"{}\", attempting to {} it...", graphIndexName, pkName, testState, action);
				try {

					IndexJobFuture future = mgmt.updateIndex(mgmt.getGraphIndex(graphIndexName), action);
					if (future == null) {
						LOG.error("updateIndex() returned a null future");
					} else {
						future.get();	// may be useless as they are sometimes empty ones
					}
					mgmt.commit();
					final GraphIndexStatusReport report = ManagementSystem.awaitGraphIndexStatus(graph, graphIndexName)
						.status(targetState)
						.timeout(this.reindexTimeoutInSecs, ChronoUnit.SECONDS)
						.call();
					final SchemaStatus newState = report.getConvergedKeys().get(pkName);
					if (newState == oldState) {
						throw new SchemaManagementException("Unable to change index \"" + graphIndexName + "\" state for property \"" + pkName +
								"\" using action " + action +
								", index is still in state " + oldState);
					}
					LOG.info("Index \"{}\" status report for property \"{}\": {}", graphIndexName, pkName, report.toString());
				} catch (Exception e) {
					throw new SchemaManagementException("Unable to change index \"" + graphIndexName
							+ "\" state for property \"" + pkName + "\" using action " + action, e);
				} finally {
				    if (mgmt.isOpen()) {
				        mgmt.rollback();
				    }
				}
			} else {
		        mgmt.rollback();
			}
		}
	}

	private void verifyExistingGraphElements(JanusGraph graph, GraphState graphState, GraphSchemaDef graphDef)
			throws SchemaManagementException {

		verifyProperties(graph, graphState);
		verifyVertices(graph, graphState);
		verifyEdges(graph, graphState);
		verifyIndexes(graph, graphState);
		verifyLocalPropertyIndexes(graph, graphState);
		verifyLocalEdgeIndexes(graph, graphState);
	}

	private void populateNewGraphElements(JanusGraph graph, GraphState graphState, GraphSchemaDef graphDef)
			throws SchemaManagementException {

		populateNewProperties(graph, graphState);
		populateNewVertices(graph, graphState);
		populateNewEdges(graph, graphState);
		populateNewIndexes(graph, graphState);
		populateNewLocalPropIndexes(graph, graphState);
		populateNewLocalEdgeIndexes(graph, graphState);
		populateGraphMetadata(graph, graphState);
	}

	private void verifyVertices(JanusGraph graph, GraphState graphState) throws SchemaManagementException {
		graph.tx().rollback();
		final JanusGraphManagement mgmt = graph.openManagement();
		for (final SchemaVertexLabel vertexDef : graphState.getGraphSchemaDef().getVertices()) {
			final String vertexLabelName = vertexDef.getLabel();
			final VertexLabel dbVertexLabel = mgmt.getVertexLabel(vertexLabelName);
			if (dbVertexLabel == null) {
				LOG.debug("Vertex {} is not found in the graph", vertexLabelName);
				if (!graphState.addPendingElement(ElementType.VERTEX, vertexLabelName)) {
					throw new SchemaManagementException("Duplicate vertex \"" + vertexLabelName + "\"");
				}
				continue;
			}
			LOG.debug("Verifying vertex {}", vertexLabelName);
			assertGraphSetting("vertex", vertexLabelName, "partition", vertexDef.getPartition(),
					Boolean.valueOf(dbVertexLabel.isPartitioned()));
			assertGraphSetting("vertex", vertexLabelName, "static", vertexDef.getStatic(),
					Boolean.valueOf(dbVertexLabel.isStatic()));
			graphState.addElement(ElementType.VERTEX, vertexLabelName);
		}
		mgmt.rollback();
	}

	private void verifyEdges(JanusGraph graph, GraphState graphState) throws SchemaManagementException {
		final JanusGraphManagement mgmt = graph.openManagement();
		for (final SchemaEdgeLabel edgeDef : graphState.getGraphSchemaDef().getEdges()) {
			final String edgeLabelName = edgeDef.getLabel();
			final EdgeLabel dbEdgeLabel = mgmt.getEdgeLabel(edgeLabelName);
			if (dbEdgeLabel == null) {
				LOG.debug("Edge {} is not found in the graph", edgeLabelName);
				if (!graphState.addPendingElement(ElementType.EDGE, edgeLabelName)) {
					throw new SchemaManagementException("Duplicate edge \"" + edgeLabelName + "\"");
				}
				continue;
			}
			LOG.debug("Verifying edge {}", edgeLabelName);
			assertGraphSetting("edge", edgeLabelName, "unidirected", edgeDef.getUnidirected(),
					!Boolean.valueOf(dbEdgeLabel.isDirected()));
			assertGraphSetting("edge", edgeLabelName, "multiplicity", edgeDef.getMultiplicity(),
					dbEdgeLabel.multiplicity());
			graphState.addElement(ElementType.EDGE, edgeLabelName);
		}
		mgmt.rollback();
	}

	private void verifyProperties(JanusGraph graph, GraphState graphState) throws SchemaManagementException {
		final JanusGraphManagement mgmt = graph.openManagement();
		for (final SchemaPropertyDef propertyDef : graphState.getGraphSchemaDef().getProperties()) {
			final String propertyKey = propertyDef.getKey();
			final PropertyKey dbPropertyKey = mgmt.getPropertyKey(propertyKey);
			if (dbPropertyKey == null) {
				LOG.debug("Property {} is not found in the graph", propertyKey);
				if (!graphState.addPendingElement(ElementType.PROPERTY, propertyKey)) {
					throw new SchemaManagementException("Duplicate property \"" + propertyKey + "\"");
				}
				continue;
			}
			LOG.debug("Verifying property {}", propertyKey);

			assertGraphSetting("property", propertyKey, "cardinality",
					ObjectUtils.defaultIfNull(propertyDef.getCardinality(), Cardinality.SINGLE),
					dbPropertyKey.cardinality());
			assertGraphSetting("property", propertyKey, "data type", propertyDef.getDataType(),
					dbPropertyKey.dataType().getName());
			graphState.addElement(ElementType.PROPERTY, propertyKey);
		}
		mgmt.rollback();
	}

	private void verifyIndexes(JanusGraph graph, GraphState graphState) throws SchemaManagementException {
		final JanusGraphManagement mgmt = graph.openManagement();
		for (final GraphIndexDef indexDef : graphState.getGraphSchemaDef().getGraphIndexes()) {
			final String indexName = indexDef.getName();
			final JanusGraphIndex dbGraphIndex = mgmt.getGraphIndex(indexName);
			if (dbGraphIndex == null) {
				LOG.debug("Index {} is not found in the graph", indexName);
				if (!graphState.addPendingElement(ElementType.INDEX, indexName)) {
					throw new SchemaManagementException("Duplicate graph index \"" + indexName + "\"");
				}
				continue;
			}
			LOG.debug("Verifying index {}", indexName);
			final Class<? extends Element> JanusGraphIndexClass;
			switch (indexDef.getRelType()) {
			case EDGE:
				JanusGraphIndexClass = JanusGraphEdge.class;
				break;
			case VERTEX:
				JanusGraphIndexClass = JanusGraphVertex.class;
				break;
			default:
				// not expected
				throw new RuntimeException("Unsupported index relation: " + indexDef.getRelType());
			}
			assertGraphSetting("index", indexName, "relation type", JanusGraphIndexClass, dbGraphIndex.getIndexedElement());
			assertGraphSetting("index", indexName, "type", indexDef.getIndexType(), dbGraphIndex.isCompositeIndex()
					? GraphIndexDef.IndexType.COMPOSITE : GraphIndexDef.IndexType.MIXED);
			assertGraphSetting("index", indexName, "unique", indexDef.getUnique(),
					Boolean.valueOf(dbGraphIndex.isUnique()));
			if (dbGraphIndex.isMixedIndex()) {
				assertGraphSetting("index", indexName, "index backend",
						getIndexingBackendName(graphState.getGraphSchemaDef(), indexDef),
						dbGraphIndex.getBackingIndex());
			}

			// NOTE: the keys in the index are not sorted so they can appear in any order
			final List<String> declaredKeys = indexDef.getKeys().stream().map(GraphIndexKeyDef::getKey).sorted()
					.collect(Collectors.toList());
			final List<String> dbKeys = Arrays.stream(dbGraphIndex.getFieldKeys()).map(PropertyKey::name).sorted()
					.collect(Collectors.toList());

			assertGraphSetting("index", indexName, "property keys", declaredKeys, dbKeys);

			graphState.addElement(ElementType.INDEX, indexName);

			for(final PropertyKey pk: dbGraphIndex.getFieldKeys()) {
				final SchemaStatus status = dbGraphIndex.getIndexStatus(pk);
				if (status != SchemaStatus.ENABLED) {
					LOG.warn("Current index status for property \"{}\" of index \"{}\" is {}", pk.name(), indexName,
							status);
				}
			}
		}
		mgmt.rollback();
	}

	private void verifyLocalPropertyIndexes(JanusGraph graph, GraphState graphState) throws SchemaManagementException {
		final JanusGraphManagement mgmt = graph.openManagement();
		for (final LocalPropertyIndexDef indexDef : graphState.getGraphSchemaDef().getLocalPropertyIndexes()) {
			final String indexName = indexDef.getName();
			final PropertyKey targetProperty = mgmt.getPropertyKey(indexDef.getKey());

			if (targetProperty == null) {
				if (!graphState.pendingElementExists(ElementType.PROPERTY, indexDef.getKey())) {
					throw new SchemaManagementException("Local property index \"" + indexName
						+ "\" refers to non-existing property key \"" + indexDef.getKey() + "\"");
				} else {
					// we can't really validate the index for the property that does not exist yet
					continue;
				}
			}

			final RelationTypeIndex dbPropertyIndex = mgmt.getRelationIndex(targetProperty, indexName);
			if (dbPropertyIndex == null) {
				LOG.debug("Local property index {} is not found in the graph", indexName);
				if (!graphState.addPendingElement(ElementType.LOCAL_INDEX, indexName)) {
					throw new SchemaManagementException("Duplicate local property index \"" + indexName + "\"");
				}
				continue;
			}
			final SchemaSortKey sortKey = indexDef.getSortKey();

			final List<String> dbSortKeys = Arrays.stream(dbPropertyIndex.getSortKey()).map(RelationType::name)
					.collect(Collectors.toList());

			assertGraphSetting("local property index", indexName, "sort key", sortKey.getKeys(), dbSortKeys);

			assertGraphSetting("local property index", indexName, "sort order", sortKey.getOrder().getTP(),
					dbPropertyIndex.getSortOrder());

			final List<String> declaredKeys = indexDef.getSortKey().getKeys().stream().sorted().collect(Collectors.toList());
			final List<String> dbKeys = Arrays.stream(dbPropertyIndex.getSortKey()).map(RelationType::name).sorted()
					.collect(Collectors.toList());

			assertGraphSetting("local property index", indexName, "property keys", declaredKeys, dbKeys);

			graphState.addElement(ElementType.LOCAL_INDEX, indexName);

			final SchemaStatus status = dbPropertyIndex.getIndexStatus();
			if (status != SchemaStatus.ENABLED) {
				LOG.warn("Current index status for local property index \"{}\" is {}", indexName,
						status);
			}
		}

		mgmt.rollback();
	}

	private void verifyLocalEdgeIndexes(JanusGraph graph, GraphState graphState) throws SchemaManagementException {
		final JanusGraphManagement mgmt = graph.openManagement();
		for (final LocalEdgeIndexDef indexDef : graphState.getGraphSchemaDef().getLocalEdgeIndexes()) {
			final String indexName = indexDef.getName();
			final EdgeLabel targetEdge = mgmt.getEdgeLabel(indexDef.getLabel());
			if (targetEdge == null) {
				if (!graphState.pendingElementExists(ElementType.EDGE, indexDef.getLabel())) {
					throw new SchemaManagementException("Local edge index \"" + indexName
							+ "\" refers to non-existing edge label \"" + indexDef.getLabel() + "\"");
				} else {
					// we can't really validate the index for the edge that does not exist yet
					continue;
				}
			}
			final RelationTypeIndex dbEdgeIndex = mgmt.getRelationIndex(targetEdge, indexName);
			if (dbEdgeIndex == null) {
				LOG.debug("Local edge index {} is not found in the graph", indexName);
				if (!graphState.addPendingElement(ElementType.LOCAL_INDEX, indexName)) {
					throw new SchemaManagementException("Duplicate local property index \"" + indexName + "\"");
				}
				continue;
			}
			final SchemaSortKey sortKey = indexDef.getSortKey();

			final List<String> dbSortKeys = Arrays.stream(dbEdgeIndex.getSortKey()).map(RelationType::name).sorted()
					.collect(Collectors.toList());
			final List<String> declaredKeys = sortKey.getKeys().stream().sorted().collect(Collectors.toList());

			assertGraphSetting("local edge index", indexName, "sort key", declaredKeys, dbSortKeys);

			assertGraphSetting("local edge index", indexName, "sort order", sortKey.getOrder().getTP(),
					dbEdgeIndex.getSortOrder());

			assertGraphSetting("local edge index", indexName, "direction", indexDef.getDirection(),
					dbEdgeIndex.getDirection());

			graphState.addElement(ElementType.LOCAL_INDEX, indexName);

			final SchemaStatus status = dbEdgeIndex.getIndexStatus();
			if (status != SchemaStatus.ENABLED) {
				LOG.warn("Current index status for local property index \"{}\" is {}", indexName,
						status);
			}
		}

		mgmt.rollback();
	}

	private void assertGraphSetting(String relType, String relName, String relProp, Object schemaVal, Object dbVal)
			throws SchemaManagementException {
		LOG.debug("Verifying relation type={}, relation name={}, property={}, schema value={}, database value={}",
				new Object[] { relType, relName, relProp, schemaVal, dbVal });
		if (!ObjectUtils.equals(schemaVal, dbVal)) {
			throw new SchemaManagementException(String.format(
					"Existing graph relation violates the schema: relation type=%s, "
							+ "relation name=%s, property=%s, schema value=%s, database value=%s",
					relType, relName, relProp, schemaVal, dbVal));
		}
	}

	private void populateNewVertices(JanusGraph graph, GraphState graphState) throws SchemaManagementException {
		graph.tx().rollback();
		final JanusGraphManagement mgmt = graph.openManagement();
		for (final SchemaVertexLabel vertexDef : graphState.getGraphSchemaDef().getVertices()) {
			final String vertexLabelName = vertexDef.getLabel();

			if (graphState.elementExists(ElementType.VERTEX, vertexLabelName)) {
				LOG.debug("Vertex {} already exists, skipping", vertexLabelName);
				continue;
			}

			LOG.info("Creating vertex {}", vertexLabelName);

			final VertexLabelMaker vertexMaker = mgmt.makeVertexLabel(vertexLabelName);

			if (vertexDef.getPartition().booleanValue()) {
				vertexMaker.partition();
			}
			if (vertexDef.getStatic().booleanValue()) {
				vertexMaker.setStatic();
			}

			final VertexLabel vertex = vertexMaker.make();

			if (vertexDef.getTtl() != null) {
				if (!graphState.isTtlSupported()) {
					LOG.warn("Storage backend does not support TTL, setting ignored for vertex \"" + vertexLabelName + "\"");
				} else {
					mgmt.setTTL(vertex, vertexDef.getTtl().getDuration());
				}
			}
		}
		mgmt.commit();
	}

	private void populateNewEdges(JanusGraph graph, GraphState graphState) throws SchemaManagementException {
		graph.tx().rollback();
		final JanusGraphManagement mgmt = graph.openManagement();
		for (final SchemaEdgeLabel edgeDef : graphState.getGraphSchemaDef().getEdges()) {
			final String edgeLabelName = edgeDef.getLabel();

			if (graphState.elementExists(ElementType.EDGE, edgeLabelName)) {
				LOG.debug("Edge {} already exists, skipping", edgeLabelName);
				continue;
			}

			LOG.info("Creating edge {}", edgeLabelName);

			final EdgeLabelMaker edgeMaker = mgmt.makeEdgeLabel(edgeLabelName);

			edgeMaker.multiplicity(edgeDef.getMultiplicity());

			if (edgeDef.getInvisible()) {
				if (edgeMaker instanceof StandardRelationTypeMaker) {
					((StandardRelationTypeMaker)edgeMaker).invisible();
				}
			}
			if (edgeDef.getUnidirected()) {
				edgeMaker.unidirected();
			}

			final List<String> edgeSignature = edgeDef.getSignature();
			if (edgeSignature != null && edgeSignature.size() > 0) {
				final PropertyKey signatureKeys[] = new PropertyKey[edgeSignature.size()];
				int keyIndex = 0;
				for(String signaturePropKey: edgeSignature) {
					final PropertyKey pk = mgmt.getPropertyKey(signaturePropKey);
					if (pk == null) {
						throw new SchemaManagementException("Unable to configure signature for edge label \"" + edgeLabelName +
								"\", property \"" + signaturePropKey + "\" not found");
					}
					signatureKeys[keyIndex++] = pk;
				}
				edgeMaker.signature(signatureKeys);
			}

			final EdgeLabel edge = edgeMaker.make();

			if (edgeDef.getTtl() != null) {
				if (!graphState.isTtlSupported()) {
					LOG.warn("Storage backend does not support TTL, setting ignored for vertex \"" + edgeLabelName + "\"");
				} else {
					mgmt.setTTL(edge, edgeDef.getTtl().getDuration());
				}
			}
		}
		mgmt.commit();
	}

	private void populateNewProperties(JanusGraph graph, GraphState graphState) throws SchemaManagementException {
		for (final SchemaPropertyDef propertyDef : graphState.getGraphSchemaDef().getProperties()) {
			final String propertyKey = propertyDef.getKey();

			if (graphState.elementExists(ElementType.PROPERTY, propertyKey)) {
				LOG.debug("Property {} already exists, skipping", propertyKey);
				continue;
			}

			LOG.info("Creating property {}", propertyKey);

			final JanusGraphManagement mgmt = graph.openManagement();

			final PropertyKeyMaker propertyMaker = mgmt.makePropertyKey(propertyKey);

			if (propertyDef.getCardinality() != null) {
				propertyMaker.cardinality(propertyDef.getCardinality());
			}

			try {
				propertyMaker.dataType(Class.forName(propertyDef.getDataType()));
			} catch (ClassNotFoundException e) {
				mgmt.rollback();
				throw new SchemaManagementException(
						"Unknown data type " + propertyDef.getDataType() + " for property \"" + propertyKey + "\"");
			}

			final PropertyKey property = propertyMaker.make();

			if (propertyDef.getTtl() != null) {
				if (!graphState.isTtlSupported()) {
					LOG.warn("Storage backend does not support TTL, setting ignored for vertex \"" + propertyKey + "\"");
				} else {
					mgmt.setTTL(property, propertyDef.getTtl().getDuration());
				}
			}

			mgmt.commit();
		}
	}

	private void populateNewIndexes(JanusGraph graph, GraphState graphState) throws SchemaManagementException {
		graph.tx().rollback();

		for (final GraphIndexDef indexDef : graphState.getGraphSchemaDef().getGraphIndexes()) {
			final String indexName = indexDef.getName();

			if (graphState.elementExists(ElementType.INDEX, indexName)) {
				LOG.debug("Graph index {} already exists, skipping", indexName);
				graphState.addIndex(indexName, false);
				continue;
			}

			LOG.info("Creating graph index {}", indexName);

			final boolean isEdgeIndex = indexDef.getRelType() == RelType.EDGE;

			final JanusGraphManagement mgmt = graph.openManagement();

			final IndexBuilder indexBuilder = mgmt.buildIndex(indexName, isEdgeIndex ? Edge.class : Vertex.class);

			final String targetIndexType = indexDef.getIndexOnly();
			if (!StringUtils.isEmpty(targetIndexType)) {
				final JanusGraphSchemaType targetType;
				if (isEdgeIndex) {
					targetType = mgmt.getEdgeLabel(targetIndexType);
				} else {
					targetType = mgmt.getVertexLabel(targetIndexType);
				}
				if (targetType == null) {
					throw new SchemaManagementException("Target index type \"" + targetIndexType + "\" not found for index \"" +
							indexName + "\"");
				}
				indexBuilder.indexOnly(targetType);
			}

			if (BooleanUtils.toBoolean(indexDef.getUnique())) {
				indexBuilder.unique();
			}

			for(final GraphIndexKeyDef indexKey: indexDef.getKeys()) {
				final PropertyKey pk = mgmt.getPropertyKey(indexKey.getKey());
				if (pk == null) {
					throw new SchemaManagementException("Property key \"" + indexKey.getKey() + "\" not found for index \"" +
							indexName + "\"");
				}
				final List<Parameter> paramList = new ArrayList<>(2);
				if (indexKey.getMapping() != null) {
					paramList.add(indexKey.getMapping().asParameter());
				}
				if (indexKey.getParameters() != null) {
					paramList.addAll(indexKey.getParameters().stream()
							.map(p -> new Parameter<>(p.getParamKey(), p.getParamValue()))
							.collect(Collectors.toList()));
				}
				if (paramList.isEmpty()) {
					indexBuilder.addKey(pk);
				} else {
					indexBuilder.addKey(pk, paramList.toArray(new Parameter[paramList.size()]));
				}
			}

			final JanusGraphIndex index = (indexDef.getIndexType() == IndexType.COMPOSITE
					? indexBuilder.buildCompositeIndex() : indexBuilder.buildMixedIndex(
							getIndexingBackendName(graphState.getGraphSchemaDef(), indexDef)));

			mgmt.commit();

			try {
				LOG.info("Waiting for the index {} to become available...", indexName);
				final GraphIndexStatusReport indexStatusReport = ManagementSystem.awaitGraphIndexStatus(graph, indexName)
						.status(SchemaStatus.REGISTERED)
						.timeout(this.reindexTimeoutInSecs, ChronoUnit.SECONDS)
						.call();

				if (indexStatusReport.getSucceeded()) {
    				LOG.info("Enabling index {}...", indexName);
    				final JanusGraphManagement mgmtIndexEnabler = graph.openManagement();
    				mgmtIndexEnabler.updateIndex(mgmtIndexEnabler.getGraphIndex(indexName), SchemaAction.ENABLE_INDEX).get();
    				mgmtIndexEnabler.commit();
    				LOG.info("Index {} is enabled, existing data may need to be reindexed", indexName);

    				graphState.addIndex(indexName, true);
			    } else {
			        throw new SchemaManagementException("Unable to register new index \"" + indexName + "\" after " +
			                indexStatusReport.getElapsed().getSeconds() + "s");
			    }
			} catch (InterruptedException e) {
				throw new SchemaManagementException("Unable to get status for index \"" + indexName + "\"", e);
			} catch (ExecutionException e) {
				throw new SchemaManagementException("Unable to enable index \"" + indexName + "\"", e);
			}
		}
	}

	private void populateNewLocalPropIndexes(JanusGraph graph, GraphState graphState) throws SchemaManagementException {
		for (final LocalPropertyIndexDef localPropIndexDef : graphState.getGraphSchemaDef().getLocalPropertyIndexes()) {
			final String indexName = localPropIndexDef.getName();

			if (graphState.elementExists(ElementType.LOCAL_INDEX, indexName)) {
				LOG.debug("Local index {} already exists, skipping", indexName);
				graphState.addIndex(indexName, false);
				continue;
			}

			LOG.info("Creating local property index {}", indexName);

			final JanusGraphManagement mgmt = graph.openManagement();

			final PropertyKey pk = mgmt.getPropertyKey(localPropIndexDef.getKey());
			if (pk == null) {
				throw new SchemaManagementException("Unable to find property \"" + localPropIndexDef.getKey() +
						"\" for building local property index \"" + indexName + "\"");
			}

			mgmt.buildPropertyIndex(pk, indexName,
					localPropIndexDef.getSortKey().getOrder().getTP(),
					localPropIndexDef.getSortKey().getKeys().stream().map(kn -> mgmt.getPropertyKey(kn))
							.collect(Collectors.toList()).toArray(new PropertyKey[0]));
			mgmt.commit();

			try {
				LOG.info("Waiting for the local property index {} to become available...", indexName);
				final RelationIndexStatusReport indexStatusReport = ManagementSystem.awaitRelationIndexStatus(graph, indexName, localPropIndexDef.getKey())
						.status(SchemaStatus.REGISTERED)
						.timeout(this.reindexTimeoutInSecs, ChronoUnit.SECONDS)
						.call();

                if (indexStatusReport.getSucceeded()) {
                    LOG.info("Enabling local property index {}...", indexName);
                    final JanusGraphManagement mgmtIndexEnabler = graph.openManagement();
                    mgmtIndexEnabler
                            .updateIndex(mgmtIndexEnabler.getRelationIndex(pk, indexName), SchemaAction.ENABLE_INDEX)
                            .get();
                    mgmtIndexEnabler.commit();
                    LOG.info("Local property index {} is enabled, existing data may need to be reindexed", indexName);

                    graphState.addIndex(indexName, true);
                } else {
                    throw new SchemaManagementException("Unable to register new local property index \"" + indexName
                            + "\" after " + indexStatusReport.getElapsed().getSeconds() + "s");
                }
			} catch (InterruptedException e) {
				throw new SchemaManagementException("Unable to get status for index \"" + indexName + "\"", e);
			} catch (ExecutionException e) {
				throw new SchemaManagementException("Unable to enable index \"" + indexName + "\"", e);
			}
		}
	}

	private void populateNewLocalEdgeIndexes(JanusGraph graph, GraphState graphState) throws SchemaManagementException {

		for (final LocalEdgeIndexDef localEdgeIndexDef : graphState.getGraphSchemaDef().getLocalEdgeIndexes()) {
			final String indexName = localEdgeIndexDef.getName();

			if (graphState.elementExists(ElementType.LOCAL_INDEX, indexName)) {
				LOG.debug("Local index {} already exists, skipping", indexName);
				graphState.addIndex(indexName, false);
				continue;
			}

			LOG.info("Creating local edge index {}", indexName);

			final JanusGraphManagement mgmt = graph.openManagement();

			final EdgeLabel el = mgmt.getEdgeLabel(localEdgeIndexDef.getLabel());
			if (el == null) {
				throw new SchemaManagementException("Unable to find edge label \"" + localEdgeIndexDef.getLabel() +
						"\" for building local property index \"" + indexName + "\"");
			}

			mgmt.buildEdgeIndex(el, indexName, localEdgeIndexDef.getDirection(),
					localEdgeIndexDef.getSortKey().getOrder().getTP(),
					localEdgeIndexDef.getSortKey().getKeys().stream().map(kn -> mgmt.getPropertyKey(kn))
							.collect(Collectors.toList()).toArray(new PropertyKey[0]));
			mgmt.commit();

			try {
				LOG.info("Waiting for the local edge index {} to become available...", indexName);
				final RelationIndexStatusReport indexStatusReport = ManagementSystem.awaitRelationIndexStatus(graph, indexName, localEdgeIndexDef.getLabel())
						.status(SchemaStatus.REGISTERED)
						.timeout(this.reindexTimeoutInSecs, ChronoUnit.SECONDS)
						.call();

                if (indexStatusReport.getSucceeded()) {
                	LOG.info("Enabling local edge index {}...", indexName);
                	final JanusGraphManagement mgmtIndexEnabler = graph.openManagement();
                	mgmtIndexEnabler.updateIndex(mgmtIndexEnabler.getRelationIndex(el, indexName), SchemaAction.ENABLE_INDEX).get();
                	mgmtIndexEnabler.commit();
                	LOG.info("Local edge index {} is enabled, existing data may need to be reindexed", indexName);

                	graphState.addIndex(indexName, true);
                } else {
                    throw new SchemaManagementException("Unable to register new local edge index \"" + indexName + "\" after " +
                            indexStatusReport.getElapsed().getSeconds() + "s");
                }
			} catch (InterruptedException e) {
				throw new SchemaManagementException("Unable to get status for index \"" + indexName + "\"", e);
			} catch (ExecutionException e) {
				throw new SchemaManagementException("Unable to enable index \"" + indexName + "\"", e);
			}
		}
	}

	private String getIndexingBackendName(GraphSchemaDef graphSchema, GraphIndexDef indexDef) throws SchemaManagementException {
		if (indexDef.getIndexType() == IndexType.COMPOSITE) {
			return null;
		}

		if (StringUtils.isEmpty(indexDef.getIndexBackend())) {
			final GraphIndexingDefaults graphIndexing = graphSchema.getGraph().getIndexing();
			if (graphIndexing != null) {
				return graphIndexing.getDefaultIndexingBackend();
			}
		}

		return indexDef.getIndexBackend();
	}

	private void populateGraphMetadata(JanusGraph graph, GraphState graphState) throws SchemaManagementException {
		GlobalMetaDataManager gdm = new GlobalMetaDataManager();
		try {
			gdm.updateGraph(graph, graphState.getGraphSchemaDef(), this);
		} catch (IOException e) {
			throw new SchemaManagementException("Failed to persist graph metadata", e);
		}
	}
}
