from RDBexplore import connect_utils

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
		return

	def dropData(self):
		self._successfulDataImport = False
		self._nodes = []
		self._edges = []
		self._importer = None
		return






	def extractData(self, databaseConnection, include_system_tables=False):
		self.dropData()
		self._system_tables_included = include_system_tables
		# add some logic here to switch between connection types.  At moment will only accept MySQL connections.
		# Also need some logic to handle unknown connections types. 
		# For other database type introduction, use same instance name, but instantiate it from a different 'Import' class from connect_utils.
		# if connection_type == 'mysql':
		self._importer = connect_utils.Import_MySQL(databaseConnection, include_system_tables=self._system_tables_included)

		try:
			self._nodes, self._edges = self._importer.getData()
			self._successfulDataImport = True
			print(f'Relational database metadata extracted using connection {str(databaseConnection)}')

		except Exception as err:
			print(f'Exception occurred in RDB_Graph.extractData() for connection {str(databaseConnection)}.  Data not extracted.')
			self._successfulDataImport = False
			raise Exception(err)

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




