package nl.maastrichtuniversity.networklibrary.cyneo4j.internal.extensionlogic.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.lang.StringBuilder;

import nl.maastrichtuniversity.networklibrary.cyneo4j.internal.utils.CyUtils;
import nl.maastrichtuniversity.networklibrary.cyneo4j.internal.utils.NeoUtils;

import org.cytoscape.model.CyEdge;
import org.cytoscape.model.CyNetwork;
import org.cytoscape.model.CyNode;
import org.cytoscape.model.CyTable;

public class CypherResultParser {

	private static final String NODE_KEY = "outgoing_typed_relationships";
	private static final String EDGE_KEY = "type";

	protected List<String> cols;
	protected Map<String,ResType> colType = new HashMap<String,ResType>();
	protected CyNetwork currNet;

	protected long numNodes;
	protected long numEdges;

	public CypherResultParser(CyNetwork network){
		this.currNet = network;

		numNodes = 0;
		numEdges = 0;
	}

	public void parseRetVal(Object callRetValue){
		Map<String,Object> retVal = (Map<String,Object>)callRetValue;

		readColumns((List<String>)retVal.get("columns"));
		readResultTable((List<List<Object>>)retVal.get("data"));
	}

	protected void readColumns(List<String> columns){
		cols = columns;
		for(String col : cols){
			colType.put(col,ResType.Unknown);
		}
	}

	protected void readResultTable(List<List<Object>> rows){

		for(int r = 0; r < rows.size(); ++r){
			List<Object> row = rows.get(r);

			for(int i = 0; i < row.size(); ++i){

				Object item = row.get(i);
				String col = cols.get(i);
				ResType type = duckTypeObject(item, col);

				switch(type){
				case Node:
					parseNode(item,col);
					break;

				case Edge:
					parseEdge(item,col);
					break;

				case EdgeCollection:
					parseEdgeCollection(item,col);
					break;

				default:
					break;

				}
			}
		}
	}

	public void parseNode(Object nodeObj, String column){

		CyTable defNodeTab = currNet.getDefaultNodeTable();
		if(defNodeTab.getColumn("neoid") == null){
			defNodeTab.createColumn("neoid", Long.class, false);
		}

		Map<String,Object> node = (Map<String,Object>)nodeObj;

		String selfURL = (String)node.get("self");
		Long self = Long.valueOf(NeoUtils.extractID((String)node.get("self")));

		CyNode cyNode = CyUtils.getNodeByNeoId(currNet, self);

		if(cyNode == null){
			cyNode = currNet.addNode();
			currNet.getRow(cyNode).set("neoid", self);
			++numNodes;
		}

		Map<String,Object> nodeProps = (Map<String,Object>) node.get("data");

		for(Entry<String,Object> obj : nodeProps.entrySet()){
			if(defNodeTab.getColumn(obj.getKey()) == null){
				if(obj.getValue().getClass() == ArrayList.class){
					defNodeTab.createListColumn(obj.getKey(), String.class, true);
				} else {
					defNodeTab.createColumn(obj.getKey(), obj.getValue().getClass(), true);
				}
			}

			Object value = CyUtils.fixSpecialTypes(obj.getValue(), defNodeTab.getColumn(obj.getKey()).getType());
			defNodeTab.getRow(cyNode.getSUID()).set(obj.getKey(), value);
		}

		try{
			Map<String,Object> nodeMetas = (Map<String,Object>) node.get("metadata");
			List<String> labels = (List<String>) nodeMetas.get("labels");
			for(int i = 0; i < labels.size(); ++i){
				String labelValue = labels.get(i);
				String labelColumnName = ((new StringBuilder()).append("neo_meta_label_").append(i).toString());

				if(defNodeTab.getColumn(labelColumnName) == null){
					defNodeTab.createColumn(labelColumnName, String.class, true);
				}
				defNodeTab.getRow(cyNode.getSUID()).set(
					labelColumnName,
					labelValue
				);
			}
		}catch(NullPointerException e){};
	}

	public void parseEdge(Object edgeObj, String column){

		CyTable defEdgeTab = currNet.getDefaultEdgeTable();
		if(defEdgeTab.getColumn("neoid") == null){
			defEdgeTab.createColumn("neoid", Long.class, false);
		}

		CyTable defNodeTab = currNet.getDefaultNodeTable();
		if(defNodeTab.getColumn("neoid") == null){
			defNodeTab.createColumn("neoid", Long.class, false);
		}

		Map<Object, Object> edge = (Map<Object, Object>)edgeObj;

		String selfURL = (String)edge.get("self");
		Long self = NeoUtils.extractID(selfURL);

		CyEdge cyEdge = CyUtils.getEdgeByNeoId(currNet, self);

		if(cyEdge == null){

			String startUrl = (String)edge.get("start");
			Long start = NeoUtils.extractID(startUrl);

			String endUrl = (String)edge.get("end");
			Long end = NeoUtils.extractID(endUrl);

			String type = (String)edge.get("type");

			CyNode startNode = CyUtils.getNodeByNeoId(currNet, start);
			CyNode endNode = CyUtils.getNodeByNeoId(currNet, end);

			if(startNode == null){
				startNode = currNet.addNode();
				currNet.getRow(startNode).set("neoid", start);
			}

			if(endNode == null){
				endNode = currNet.addNode();
				currNet.getRow(endNode).set("neoid", end);
			}

			cyEdge = currNet.addEdge(startNode, endNode, true);
			++numEdges;

			currNet.getRow(cyEdge).set("neoid", self);
			currNet.getRow(cyEdge).set(CyEdge.INTERACTION, type);

			Map<String,Object> nodeProps = (Map<String,Object>) edge.get("data");

			for(Entry<String,Object> obj : nodeProps.entrySet()){
				if(defEdgeTab.getColumn(obj.getKey()) == null){
					if(obj.getValue().getClass() == ArrayList.class){
						defEdgeTab.createListColumn(obj.getKey(), String.class, true);
					} else {
						defEdgeTab.createColumn(obj.getKey(), obj.getValue().getClass(), true);
					}
				}

				Object value = CyUtils.fixSpecialTypes(obj.getValue(), defEdgeTab.getColumn(obj.getKey()).getType());
				defEdgeTab.getRow(cyEdge.getSUID()).set(obj.getKey(), value);

			}
		}
	}

	public void parseEdgeCollection(Object edgeCollectionObj, String column){
		List<Object> edgeCollection = (List<Object>) edgeCollectionObj;
		for(Object edge: edgeCollection){
			parseEdge(edge, column);
		}
	}

	public long nodesAdded(){
		return numNodes;
	}

	public long edgesAdded() {
		return numEdges;
	}

	protected ResType duckTypeObject(Object obj,String column){

		ResType result = colType.get(column);

		if(result == ResType.Unknown){
			if(isNodeType(obj)){
				result = ResType.Node;
			} else if(isEdgeType(obj)){
				result = ResType.Edge;
			} else if(isEdgeCollectionType(obj)){
				result = ResType.EdgeCollection;
			} else { // this could / should be extended
				result = ResType.Ignore;
			}
			colType.put(column, result);
		}

		return result;
	}

	protected boolean isNodeType(Object obj){
		try{
			Map<String,Object> node = (Map<String,Object>)obj;
			return node.containsKey(NODE_KEY);

		} catch(ClassCastException e){
			return false;
		}
	}

	protected boolean isEdgeType(Object obj){
		try{
			Map<String,Object> node = (Map<String,Object>)obj;
			return node.containsKey(EDGE_KEY);

		} catch(ClassCastException e){
			return false;
		}
	}

	protected boolean isEdgeCollectionType(Object obj){
		try{
			List<Object> nodes = (List<Object>)obj;
			for(Object node: nodes){
				if(!isEdgeType(node)){
					return false;
				}
			}
			return true;
		} catch(ClassCastException e){
			return false;
		}
	}

	protected enum ResType {
		Node,
		Edge,
		EdgeCollection,
		Ignore,
		Unknown
	}


}
