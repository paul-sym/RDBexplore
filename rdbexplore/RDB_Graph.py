import itertools

from rdbexplore import connect_utils
import networkx as nx


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
		self._successfulTableDataImport = False
		self._nodes = []
		self._edges = []
		self._importer = None
		self._exporter = None
		self._tableOnlyGraph = None
		return

	def dropData(self):
		self._successfulDataImport = False
		self._successfulTableDataImport = False
		self._nodes = []
		self._edges = []
		self._importer = None
		self._tableOnlyGraph = None
		return






	def extractData(self, databaseConnection, connectionType, include_system_tables=False, specific_schema=None):
		self.dropData()
		self._system_tables_included = include_system_tables
		
		# block setting which database type to use (and therefore which class to use as '_importer')
		if connectionType == 'mysql':
			self._importer = connect_utils.Import_MySQL(databaseConnection, include_system_tables=self._system_tables_included, specific_schema=specific_schema)
		elif connectionType == 'oracle':
			self._importer = connect_utils.Import_Oracle(databaseConnection, include_system_tables=self._system_tables_included, specific_schema=specific_schema)
		else:
			raise Exception("Connection type not recognised.  Use one of the following types: 'mysql', 'oracle'.")



		try:
			self._nodes, self._edges, self._tableOnlyGraph = self._importer.getData()
			self._successfulDataImport = True
			self._successfulTableDataImport = True
			print(f'Relational database metadata extracted using connection {str(databaseConnection)}')

		except Exception as err:
			print(f'Exception occurred in RDB_Graph.extractData() for connection {str(databaseConnection)}.  Data not extracted.')
			print(err)
		return




	def extractDataCSV(self, schema_query_path, table_query_path, column_query_path, constraint_query_path):
		self.dropData()
		self._importer = connect_utils.Import_From_CSV(schema_query_path, table_query_path, column_query_path, constraint_query_path)

		try:
			self._nodes, self._edges, self._tableOnlyGraph = self._importer.getData()
			self._successfulDataImport = True
			self._successfulTableDataImport = True
			print(f'Metadata extracted using CSV.')

		except Exception as err:
			print(f'Exception occurred in RDB_Graph.extractData() for csv file.  Data not extracted.')
			print(err)
		return




	def generateShortestJoinPathOneWay(self, table1_id, table2_id, printPath=True, returnAsSQL=False):
		if self._successfulTableDataImport:
			try:
				shortestPathNodesForward = nx.algorithms.shortest_paths.generic.shortest_path(self._tableOnlyGraph, source=table1_id, target=table2_id)
			except: 
				shortestPathNodesForward = []
			try:
				shortestPathNodesReverse = nx.algorithms.shortest_paths.generic.shortest_path(self._tableOnlyGraph, target=table1_id, source=table2_id)
			except:
				shortestPathNodesReverse = []
			
			if len(shortestPathNodesForward) <= len(shortestPathNodesReverse) and len(shortestPathNodesForward) > 0: shortestPathNodes = shortestPathNodesForward
			elif len(shortestPathNodesForward) > len(shortestPathNodesReverse) and len(shortestPathNodesReverse) > 0: shortestPathNodes = shortestPathNodesReverse
			else: return []

			if printPath:
				for i in range(0,len(shortestPathNodes)-1):
					print(shortestPathNodes[i])
					print('\t\t|')
					print('\t\tv')
				print(shortestPathNodes[-1])

			# use data to create a join statement
			if returnAsSQL:
				sql_join_output = f"SELECT {shortestPathNodes[0]}.*, {shortestPathNodes[-1]}.*  \n\nFROM {shortestPathNodes[0]}"
				for i in range(1, len(shortestPathNodes)):
					fromColumn = self._tableOnlyGraph.get_edge_data(shortestPathNodes[i-1], shortestPathNodes[i])['column_id']
					toColumn = self._tableOnlyGraph.get_edge_data(shortestPathNodes[i-1], shortestPathNodes[i])['referenced_column_id']
					sql_join_output = sql_join_output + f" \nJOIN {shortestPathNodes[i]} ON {fromColumn} = {toColumn}"

				where_restriction = '\n' + str(where_restriction)
				sql_join_output = sql_join_output + f'{where_restriction};'

				return shortestPathNodes, sql_join_output

			return shortestPathNodes



		else:
			print('No data imported.  Use "getData" function to import data.')
		return


	def generateShortestJoinPath(self, table1_id, table2_id, printPath=True, returnAsSQL=False, where_restriction=''):
		"""
		This method calculates the shortest path between two tables and constructs a working join statement for these.  
		Note: the statement presented does not account for any databse factors (e.g. indexing, row length etc in its calculation).
		"""
		if self._successfulTableDataImport:
			tableOnlyGraphUndirected = self._tableOnlyGraph.to_undirected() # converts to an undirected graph (as joins can be done regardless of reference direction)
			try:
				shortestPathNodes = nx.algorithms.shortest_paths.generic.shortest_path(tableOnlyGraphUndirected, source=table1_id, target=table2_id)
			except:
				shortestPathNodes = []
				return shortestPathNodes

			if printPath:
				for i in range(0,len(shortestPathNodes)-1):
					print(shortestPathNodes[i])
					print('\t\t|')
					print('\t\tv')
				print(shortestPathNodes[-1])

			# use data to create a join statement
			if returnAsSQL:
				sql_join_output = f"SELECT {shortestPathNodes[0]}.*, {shortestPathNodes[-1]}.*  \n\nFROM {shortestPathNodes[0]}"
				for i in range(1, len(shortestPathNodes)):
					fromColumn = tableOnlyGraphUndirected.get_edge_data(shortestPathNodes[i-1], shortestPathNodes[i])['column_id']
					toColumn = tableOnlyGraphUndirected.get_edge_data(shortestPathNodes[i-1], shortestPathNodes[i])['referenced_column_id']
					sql_join_output = sql_join_output + f" \nJOIN {shortestPathNodes[i]} ON {fromColumn} = {toColumn}"

				where_restriction = '\n' + str(where_restriction)
				sql_join_output = sql_join_output + f'{where_restriction};'

				return shortestPathNodes, sql_join_output

			return shortestPathNodes

		else:
			raise Exception('No data imported.  Use "getData" function to import data.')
			return


	def findMostRootTables(self, numberToReturn=0):


		if self._successfulTableDataImport:
			
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
		if self._successfulTableDataImport:
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
		if self._successfulTableDataImport:
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


	def saveGraph(self, filePathToSaveTo):
		if self._successfulTableDataImport:
			nx.readwrite.graphml.write_graphml(self._tableOnlyGraph, filePathToSaveTo)
			print('Exported in GraphML format.')
		else:
			print('No data imported.  Use "getData" function to import data.')
		return


	def loadGraph(self, filePathToLoadFrom, node_type=str, edge_key_type=str):
		self._tableOnlyGraph = nx.readwrite.graphml.read_graphml(filePathToLoadFrom)
		self._successfulDataImport = False
		self._successfulTableDataImport = True
		print('Database metadata imported.')
		return


	def findNeighbours(self, node, printOutput=False):
		if self._successfulTableDataImport:
			neighbours = []
			for key, val in self._tableOnlyGraph[node].items():
				neighbours.append(key)
				if printOutput: print (key)
			return neighbours

		else:
			print('No data imported.  Use "getData" function to import data.')
			return
		








