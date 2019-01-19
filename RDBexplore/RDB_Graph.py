from rdbexplore import connect_utils
import networkx as nx
import itertools

class RDB_Graph(object):


	@property 		# corresponding 'setter' deliberately omitted
	def successfulDataImport(self):
		return self._successfulDataImport

	@property 		# corresponding 'setter' deliberately omitted
	def system_tables_included(self):
		return self._system_tables_included

	@property
	def nodes(self):
		if self._successfulDataImport:
			return self._nodes
		else:
			return None

	@property
	def edges(self):
		if self._successfulDataImport:
			return self._edges
		else:
			return None
	
	
	
	



	def __init__(self):
		self._successfulDataImport = False
		self._nodes = []
		self._edges = []
		self._importer = None
		self._exporter = None
		self._tableOnlyGraph = None
		return

	def dropData(self):
		self._successfulDataImport = False
		self._nodes = []
		self._edges = []
		self._importer = None
		self._tableOnlyGraph = None
		return






	def extractData(self, databaseConnection, include_system_tables=False, specific_schema=None):
		self.dropData()
		self._system_tables_included = include_system_tables
		# add some logic here to switch between connection types.  At moment will only accept MySQL connections.
		# Also need some logic to handle unknown connections types. 
		# For other database type introduction, use same instance name, but instantiate it from a different 'Import' class from connect_utils.
		# if connection_type == 'mysql':
		self._importer = connect_utils.Import_MySQL(databaseConnection, include_system_tables=self._system_tables_included, specific_schema=specific_schema)

		try:
			self._nodes, self._edges, self._tableOnlyGraph = self._importer.getData()
			self._successfulDataImport = True
			print(f'Relational database metadata extracted using connection {str(databaseConnection)}')

		except Exception as err:
			print(f'Exception occurred in RDB_Graph.extractData() for connection {str(databaseConnection)}.  Data not extracted.')
			self._successfulDataImport = False
			raise Exception(err)

		return




	def generateShortestJoinPathSQL(self, table1_id, table2_id, where_restriction=''):
		"""
		This method calculates the shortest path between two tables and constructs a working join statement for these.  
		Note: the statement presented does not account for any databse factors (e.g. indexing, row length etc in its calculation).
		"""
		if self._successfulDataImport:
			tableOnlyGraphUndirected = self._tableOnlyGraph.to_undirected() # converts to an undirected graph (as joins can be done regardless of reference direction)
			shortestPathNodes = nx.algorithms.shortest_paths.generic.shortest_path(tableOnlyGraphUndirected, source=table1_id, target=table2_id)

			# use data to create a join statement
			sql_join_output = f"SELECT {shortestPathNodes[0]}.*, {shortestPathNodes[-1]}.*  \n\nFROM {shortestPathNodes[0]}"
			for i in range(1, len(shortestPathNodes)):
				fromColumn = tableOnlyGraphUndirected.get_edge_data(shortestPathNodes[i-1], shortestPathNodes[i])['column_id']
				toColumn = tableOnlyGraphUndirected.get_edge_data(shortestPathNodes[i-1], shortestPathNodes[i])['referenced_column_id']
				sql_join_output = sql_join_output + f" \nJOIN {shortestPathNodes[i]} ON {fromColumn} = {toColumn}"

			where_restriction = '\n' + str(where_restriction)
			sql_join_output = sql_join_output + f'{where_restriction};'

			return sql_join_output
		else:
			raise Exception('No data imported.  Use "getData" function to import data.')
			return




	def findMostRootTables(self, numberToReturn=0):


		if self._successfulDataImport:
			
			# loop through each of the table nodes, finding the in and out degrees (and taking the ratio too)
			degrees = []
			for node in self._tableOnlyGraph.nodes:
				in_deg = self._tableOnlyGraph.in_degree(node)
				out_deg = self._tableOnlyGraph.out_degree(node)
				if out_deg == 0: deg_ratio = in_deg
				else: deg_ratio = in_deg/out_deg
				degrees.append([node, deg_ratio])

			degrees.sort(key=lambda x: x[1], reverse=True)

			if numberToReturn==0 or numberToReturn>len(degrees):
				return degrees
			else:
				return degrees[:numberToReturn]

		# should really write this information back to the main digraph at the end of this so that it can be stored
		else:
			raise Exception('No data imported.  Use "getData" function to import data.')
			return


	def findTableCommunities(self, maxNumberOfCommunities):
		if self._successfulDataImport:
			comm_list = []
			comp = nx.algorithms.community.centrality.girvan_newman(self._tableOnlyGraph.to_undirected())
			limited = itertools.takewhile(lambda c: len(c) <= maxNumberOfCommunities, comp)
			for communities in limited:
				comm_list.append(tuple(sorted(c) for c in communities))
			
			for i in comm_list[-1]:
				print(i)
				print('\n\n')

		# need to do something with this information now it's ben calculated

		else:
			raise Exception('No data imported.  Use "getData" function to import data.')
		return


	def exportTableOnlyGraph(self, neo4j_graphDatabase_driver):
		if self._successfulDataImport:
			# more logic needed here to hanlde other types of graph DB connection request
			self._exporter = connect_utils.Export_Neo(neo4j_graphDatabase_driver)
			self._exporter.dropAll()
			self._exporter.createTableNodes(self._tableOnlyGraph.nodes.data())
			self._exporter.createTableEdges(self._tableOnlyGraph.edges.data())
			print('Exported to graph platform successfully.')
		else:
			raise Exception('No data imported.  Use "getData" function to import data.')
		return




	def exportGraph(self, neo4j_graphDatabase_driver):
		if self._successfulDataImport:
			# more logic needed here to hanlde other types of graph DB connection request
			self._exporter = connect_utils.Export_Neo(neo4j_graphDatabase_driver)
			self._exporter.dropAll()
			self._exporter.createNodes(self._nodes)
			self._exporter.createEdges(self._edges)
			print('Exported to graph platform successfully.')
		else:
			raise Exception('No data imported.  Use "getData" function to import data.')
		return






