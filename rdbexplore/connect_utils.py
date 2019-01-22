import re
import networkx as nx

# ------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------
#		START OF IMPORT CONNECTOR CLASSES
# ------------------------------------------------------------------------------------------------------------------------
# ------------------------------------------------------------------------------------------------------------------------


class _Import_Master(object):
	def __init__(self, database_connection, include_system_tables, specific_schema=None):
		if include_system_tables == True or include_system_tables == False:
			self._include_system_tables = include_system_tables
		else: 
			raise Exception("The 'include_system_tables' value must be boolean.")
			return
		self.database_connection = database_connection
		self._specificSchema = specific_schema
		return

	@property
	def include_system_tables(self):
		return self._include_system_tables

	@include_system_tables.setter
	def include_system_tables(self, value):
		if value == True or value == False: self._include_system_tables = value
		else: raise Exception("The 'include_system_tables' value must be boolean.")
		return




	def getData(self):
		nodes = []
		edges = []
		tableGraph = nx.DiGraph()
		
		try:
			cur = self.database_connection.cursor()

			# extract top-level schema information into nodes
			cur.execute(self.schema_query)
			schema_resultset = cur.fetchall()
			for schema in schema_resultset:
				nodes.append({'id': str(schema[0]), 'name': str(schema[0]), 'class': 'Schema', 'attributes': {}})


			# extract table-level info into nodes (and the table->schema edges)
			cur.execute(self.table_query)
			table_resultset = cur.fetchall()
			for table in table_resultset:
				nodes.append({'id': str(table[1]) + '.' + str(table[0]), 'name': str(table[0]), 'class': 'Table', 'attributes': {'parent_schema': str(table[1]), 'rows': str(table[2]), 'format': str(table[3]), 'created': str(table[4]), 'last_updated': str(table[5]), 'engine': str(table[6])}})
				edges.append({'node1_id': str(table[1]) + '.' + str(table[0]), 'node2_id': str(table[1]), 'class': 'inSchema', 'attributes': {}})
				tableGraph.add_node((str(table[1]) + '.' + str(table[0])), name=str(table[0]), rows=str(table[2]))



			# extract column-level info into nodes (and the column->table edges)
			cur.execute(self.column_query)
			column_resultset = cur.fetchall()
			for column in column_resultset:
				nodes.append({'id': str(column[2]) + '.' + str(column[1]) + '.' + str(column[0]), 'name': str(column[0]), 'class': 'Column', 'attributes': {'parent_schema': str(column[2]), 'parent_table': str(column[1]), 'is_nullable': str(column[3]), 'is_primary': str(column[4]=='PRI'), 'is_unique': str(column[4]=='UNI' or column[4]=='PRI')}})
				edges.append({'node1_id': str(column[2]) + '.' + str(column[1]) + '.' + str(column[0]), 'node2_id': str(column[2]) + '.' + str(column[1]), 'class': 'inTable', 'attributes':{}})


			# extract foreign key information to link the columns together
			cur.execute(self.constraint_query)
			constraints_resultset = cur.fetchall()
			for constraint in constraints_resultset:
				node1_id = str(constraint[2]) + '.' + str(constraint[1]) + '.' + str(constraint[0])
				node2_id = str(constraint[5]) + '.' + str(constraint[4]) + '.' + str(constraint[3])
				edges.append({'node1_id': node1_id, 'node2_id': node2_id, 'class': 'References', 'attributes': {}})
				tableGraph.add_edge(str(constraint[2]) + '.' + str(constraint[1]), str(constraint[5]) + '.' + str(constraint[4]), column_id=node1_id, referenced_column_id=node2_id, constraint_name=str(constraint[6]))

			cur.close()

		except Exception as err:
			raise err
		
		return nodes, edges, tableGraph






class Import_MySQL(_Import_Master):
	def getData(self):
		"""
		This method sets the strings used to construct the SQL queries that is appropriate for the MySQL database implementation.
		It then call the the super() method of the generic class to actually execute the query and handle the response from the RDBMS.
		"""

		# if-else for including additional constraints on each SQL query if we do not want information about the MySQL system tables returned or just want one schema
		if self._specificSchema is not None:
			system_tables_flag_schema = f"WHERE schema_name = '{self._specificSchema}'"
			system_tables_flag_tables = f"WHERE table_schema = '{self._specificSchema}'"
			system_tables_flag_columns = f"WHERE table_schema = '{self._specificSchema}'"
			system_tables_flag_constraints = f"AND constraint_schema = '{self._specificSchema}'"
		elif self._include_system_tables == True:
			system_tables_flag_schema = '';
			system_tables_flag_tables = '';
			system_tables_flag_columns = '';
			system_tables_flag_constraints = '';
		elif self._include_system_tables == False:
			system_tables_flag_schema = "WHERE schema_name NOT IN ('sys', 'information_schema', 'performance_schema', 'mysql')"
			system_tables_flag_tables = "WHERE table_schema NOT IN ('sys', 'information_schema', 'performance_schema', 'mysql')"
			system_tables_flag_columns = "WHERE table_schema NOT IN ('sys', 'information_schema', 'performance_schema', 'mysql')"
			system_tables_flag_constraints = "AND constraint_schema NOT IN ('sys', 'information_schema', 'performance_schema', 'mysql')"
		else:
			raise Exception('Invalid parameter passed for "include_system_tables".  Value passed must be boolean.')


		# set the query statements up to be in the MySQL style.
		self.schema_query = f"SELECT schema_name FROM information_schema.schemata {system_tables_flag_schema};"
		self.table_query = f"SELECT table_name, table_schema, table_rows, row_format, create_time, update_time, engine FROM information_schema.tables  {system_tables_flag_tables};"
		self.column_query = f"SELECT column_name, table_name, table_schema, is_nullable, column_key FROM information_schema.columns  {system_tables_flag_columns};"
		self.constraint_query = f"SELECT column_name, table_name, table_schema, referenced_column_name, referenced_table_name, referenced_table_schema, constraint_name FROM information_schema.key_column_usage WHERE referenced_table_schema IS NOT NULL {system_tables_flag_constraints};"

		return super().getData()




class Import_Oracle(_Import_Master):
	def getData(self):
		"""
		This method sets the strings used to construct the SQL queries that is appropriate for the Oracle database implementation.
		It then call the the super() method of the generic class to actually execute the query and handle the response from the RDBMS.
		"""
		if self._specificSchema is not None:
			system_tables_flag_schema = f"WHERE owner = '{self._specificSchema}'"
			system_tables_flag_tables = f"AND atab.owner = '{self._specificSchema}'"
			system_tables_flag_columns = f"WHERE owner = '{self._specificSchema}'"
			system_tables_flag_columns2 = f"AND ac.owner = '{self._specificSchema}'"
			system_tables_flag_constraints = f"AND ac.owner = '{self._specificSchema}'"
		elif self._include_system_tables == True:
			system_tables_flag_schema = ''
			system_tables_flag_tables = ''
			system_tables_flag_columns = ''
			system_tables_flag_columns2 = ''
			system_tables_flag_constraints = ''
		elif self._include_system_tables == False:
			system_tables_flag_schema = f"WHERE owner NOT IN ('SYS', 'SYSTEM', 'INTERNAL', 'PUBLIC', 'PERFSTAT', 'OPS$ORACLE')"
			system_tables_flag_tables = f"AND atab.owner NOT IN ('SYS', 'SYSTEM', 'INTERNAL', 'PUBLIC', 'PERFSTAT', 'OPS$ORACLE')"
			system_tables_flag_columns = f"WHERE owner NOT IN ('SYS', 'SYSTEM', 'INTERNAL', 'PUBLIC', 'PERFSTAT', 'OPS$ORACLE')"
			system_tables_flag_columns2 = f"AND ac.owner NOT IN ('SYS', 'SYSTEM', 'INTERNAL', 'PUBLIC', 'PERFSTAT', 'OPS$ORACLE')"
			system_tables_flag_constraints = f"AND ac.owner NOT IN ('SYS', 'SYSTEM', 'INTERNAL', 'PUBLIC', 'PERFSTAT', 'OPS$ORACLE')"
		else:
			raise Exception('Invalid parameter passed for "include_system_tables".  Value passed must be boolean.')



		self.schema_query = f"SELECT DISTINCT owner from all_tables {system_tables_flag_schema};"

		self.table_query = f"""SELECT MAX(tbn), MAX(owne), MAX(rowss), MAX(row_format), MAX(cre), MAX(atm.timestamp), pth
		FROM
			(SELECT CONCAT(CONCAT(atab.owner, '.'), atab.table_name) AS pth, atab.table_name AS tbn, atab.owner AS owne, atab.num_rows AS rowss, 'ORACLE' as row_format, aobj.created as cre
    		FROM all_tables atab
    		JOIN all_objects aobj ON aobj.object_name = atab.table_name
 			WHERE aobj.object_type = 'TABLE'
    		{system_tables_flag_tables}
    		) tabs
    	LEFT JOIN all_tab_modifications atm ON tabs.tbn = atm.table_name
    	GROUP BY pth;"""

		self.column_query = f"""SELECT atc.column_name, atc.table_name, atc.owner, atc.nullable, primaries.prim
		FROM all_tab_columns atc
		LEFT JOIN
			(SELECT CONCAT(acc.owner,CONCAT('.', CONCAT(acc.table_name, CONCAT('.', acc.column_name)))) as pth, 'PRI' AS prim
			FROM all_cons_columns acc, all_constraints ac
			WHERE ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
			AND ac.owner = acc.owner
			AND ac.constraint_type = 'P'
			{system_tables_flag_columns2}
    	) primaries ON CONCAT(atc.owner,CONCAT('.', CONCAT(atc.table_name, CONCAT('.', atc.column_name)))) = primaries.pth
    	 {system_tables_flag_columns};"""

		self.constraint_query = f"""
			SELECT acc.COLUMN_NAME AS fromcolumn, acc.table_name AS fromtable, acc.OWNER AS fromowner, tocols.col AS tocolumn, tocols.tn AS totable, tocols.ow AS toowner
			FROM (
		        SELECT acc.owner AS ow, acc.TABLE_NAME AS tn, acc.COLUMN_NAME AS col, acc.CONSTRAINT_NAME AS cn
		        FROM all_cons_columns acc
		        JOIN all_constraints ac ON ac.constraint_name = acc.CONSTRAINT_NAME
		        WHERE ac.CONSTRAINT_TYPE IN ('P', 'U')
        		{system_tables_flag_constraints}
    		) tocols
    		JOIN all_constraints ac ON ac.R_CONSTRAINT_NAME = tocols.cn
    		JOIN all_cons_columns acc ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
			WHERE ac.CONSTRAINT_TYPE IN ('R')
			{system_tables_flag_constraints}
			;"""


		return super().getData() 	


		

		


		










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
			for key, val in edgeAttributesAsDict.items():
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
		return

	def createTableNodes(self, tupleOfNodes):
		with self._neo4j_driver.session() as session:
			for node in tupleOfNodes:
				session.write_transaction(self._executeStmt, self._generateCreateStmt(node_id=node[0], node_name=node[1]['name'], node_class='Table', nodeAttributesAsDict=node[1]))
		return

	def createTableEdges(self, tupleOfEdges):
		with self._neo4j_driver.session() as session:
			for edge in tupleOfEdges:
				session.write_transaction(self._executeStmt, self._generateEdgeStmt(node1_id=edge[0], node2_id=edge[1], edgeClass='References', edgeAttributesAsDict=edge[2]))

















