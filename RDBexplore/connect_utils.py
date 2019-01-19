import re

# ------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------
#		START OF IMPORT CONNECTOR CLASSES
# ------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------


class _Import_Master(object):
	def __init__(self, database_connection, include_system_tables):
		self.database_connection = database_connection
		self._include_system_tables=include_system_tables
		return

	@property
	def include_system_tables(self):
		return self._include_system_tables

	@include_system_tables.setter
	def include_system_tables(self, value):
		# add validation logic here to block non-boolean values from being passed
		self._include_system_tables = value
		return

	def getData(self):
		"""
		This method must be overidden with the specific implementation used for the database implementation.
		"""
		raise Exception('_Import_Master class should not be used to import.  Use an inherited class instead.')




class Import_MySQL(_Import_Master):
	

	

	def getData(self):
		
		nodes = []
		edges = []

		# if-else for including additional constraints on each SQL query if we do not want information about the MySQL system tables returned
		if self._include_system_tables == True:
			system_tables_flag_schema = '';
			system_tables_flag_tables = '';
			system_tables_flag_columns = '';
			system_tables_flag_constraints = '';
		elif self._include_system_tables == False:
			system_tables_flag_schema = "WHERE schema_name NOT IN ('sys', 'information_schema', 'performance_schema', 'mysql')"
			system_tables_flag_tables = "WHERE table_schema NOT IN ('sys', 'information_schema', 'performance_schema', 'mysql')"
			system_tables_flag_columns = "WHERE table_schema NOT IN ('sys', 'information_schema', 'performance_schema', 'mysql')"
			system_tables_flag_constraints = "constraint_schema NOT IN ('sys', 'information_schema', 'performance_schema', 'mysql') AND"
		else:
			raise Exception('Invalid parameter passed for "include_system_tables".  Value passed must be boolean.')


		try:
			cur = self.database_connection.cursor()

			# extract top-level schema information into nodes
			cur.execute(f"SELECT schema_name FROM information_schema.schemata {system_tables_flag_schema};")
			schema_resultset = cur.fetchall()
			for schema in schema_resultset:
				nodes.append({'id': str(schema[0]), 'name': str(schema[0]), 'class': 'Schema', 'attributes': {}})


			# extract table-level info into nodes (and the table->schema edges)
			cur.execute(f"SELECT table_name, table_schema, table_rows, row_format, create_time, update_time, engine FROM information_schema.tables {system_tables_flag_tables};")
			table_resultset = cur.fetchall()
			for table in table_resultset:
				nodes.append({'id': str(table[1]) + '.' + str(table[0]), 'name': str(table[0]), 'class': 'Table', 'attributes': {'parent_schema': str(table[1]), 'rows': str(table[2]), 'format': str(table[3]), 'created': str(table[4]), 'last_updated': str(table[5]), 'engine': str(table[6])}})
				edges.append({'node1_id': str(table[1]) + '.' + str(table[0]), 'node2_id': str(table[1]), 'class': 'inSchema', 'attributes': {}})



			# extract column-level info into nodes (and the column->table edges)
			cur.execute(f"SELECT column_name, table_name, table_schema, is_nullable, column_key FROM information_schema.columns {system_tables_flag_columns};")
			column_resultset = cur.fetchall()
			for column in column_resultset:
				nodes.append({'id': str(column[2]) + '.' + str(column[1]) + '.' + str(column[0]), 'name': str(column[0]), 'class': 'Column', 'attributes': {'parent_schema': str(column[2]), 'parent_table': str(column[1]), 'is_nullable': str(column[3]), 'is_primary': str(column[4]=='PRI'), 'is_unique': str(column[4]=='UNI' or column[4]=='PRI')}})
				edges.append({'node1_id': str(column[2]) + '.' + str(column[1]) + '.' + str(column[0]), 'node2_id': str(column[2]) + '.' + str(column[1]), 'class': 'inTable', 'attributes':{}})


			# extract foreign key information to link the columns together
			cur.execute(f"""
			SELECT column_name, table_name, table_schema, referenced_column_name, referenced_table_name, referenced_table_schema
			FROM information_schema.key_column_usage 
			WHERE {system_tables_flag_constraints} referenced_table_schema IS NOT NULL;
			""")
			constraints_resultset = cur.fetchall()
			for constraint in constraints_resultset:
				node1_id = constraint[2] + '.' + constraint[1] + '.' + constraint[0]
				node2_id = constraint[5] + '.' + constraint[4] + '.' + constraint[3]
				edges.append({'node1_id': node1_id, 'node2_id': node2_id, 'class': 'References', 'attributes': {}})

			cur.close()

		except Exception as err:
			raise err
		
		return nodes, edges








# ------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------
#		START OF EXPORT CONNECTOR CLASSES
# ------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------



class Export_Neo(object):
	def __init__(self, neo4j_driver):
		self._neo4j_driver = neo4j_driver
		return



	#	------------------------------------------------------------------------------------------------------------------------------------------
	#	Start of generic statement executers with Neo4j
	#	------------------------------------------------------------------------------------------------------------------------------------------


	@staticmethod
	def _executeStmt(tx, stmt):
		tx.run(stmt)
		return

	@staticmethod
	def _readStmt(tx, stmt):
		results = tx.run(stmt)
		return


	#	------------------------------------------------------------------------------------------------------------------------------------------
	#	Start of section of generic statement generators - these just perform basic string editing to generate the Cyper command properly
	#	------------------------------------------------------------------------------------------------------------------------------------------

	@staticmethod
	def _generateCreateStmt(node_id, node_name, node_class, nodeAttributesAsDict):
	    """
	    Generates a statement for creating a node in Neo4j.  Arguments are:
	    - nodeClass: single string for the class of the node generated
	    - nodeAttributesAsDict: dictionary of key-value pairs to assign to the node as attributes 
	    """
	    stmt = f"CREATE (a: {node_class} {'{'}id: '{node_id}', name: '{node_name}'"
	    if len(nodeAttributesAsDict) > 0:
		    for key, val in nodeAttributesAsDict.items():
		        stmt = stmt + ', ' + key + ': ' + "'" + val + "'"
	    stmt = stmt + '})'
	    return stmt


	@staticmethod
	def _generateEdgeStmt(node1_id, node2_id, edgeClass, edgeAttributesAsDict):
		stmt = f"""MATCH (a {'{'}id: '{node1_id}'{'}'})
				MATCH (b {'{'}id: '{node2_id}'{'}'})
				MERGE (a)-[:{edgeClass} {'{'}"""
		if len(edgeAttributesAsDict) > 0:
			for key, val in edgeAttributesAsDict:
				stmt = stmt + key + ': ' + "'" + val + "', "
			stmt = stmt[:-2] 
		stmt = stmt + '}]->(b);'
		return stmt


	#	------------------------------------------------------------------------------------------------------------------------------------------
	#	Start of public methods to generate the neo4j graph
	#	------------------------------------------------------------------------------------------------------------------------------------------


	def dropAll(self):
		with self._neo4j_driver.session() as session:
			session.write_transaction(self._executeStmt, 'MATCH (a) DETACH DELETE a;')


	def createNodes(self, listOfNodes):
		with self._neo4j_driver.session() as session:
			for node in listOfNodes:
				session.write_transaction(self._executeStmt, self._generateCreateStmt(node['id'], node['name'], node['class'], node['attributes']))
		return

	def createEdges(self, listOfEdges):
		with self._neo4j_driver.session() as session:
			for edge in listOfEdges:
				session.write_transaction(self._executeStmt, self._generateEdgeStmt(edge['node1_id'], edge['node2_id'], edge['class'], edge['attributes']))











