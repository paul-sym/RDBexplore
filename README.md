# rdbexplore
A python package to help explore and analyse enterprise scale relational databases.


The package constructs an internal graph from the RDBMS metadata, allowing the database to be analysed.  While the analysis functions can be carried out within the package, the information can also be exported to native graph software.

<br>

**Quick start example showing import from MySQL database and export to Neo4j:**

    import rdbexplore as re
    
    # create a mysql database connection to explore
    import mysql.connector as connector
    mysql_database_connection = connector.connect(**config)
    
    # create a neo4j driver object to export the graph to
    from neo4j import GraphDatabase 
    neo4j_graphDatabase_driver = GraphDatabase.driver(uri, auth=(user, password)
    
    
    explorer = re.RDB_Graph()
    explorer.extractData(mysql_database_connection)
    explorer.exportGraph(neo4j_graphDatabase_driver)
    
    


Currently supported relational database implementations:

- MySQL 8.x
- Oracle


Currently supported graph software distributions:

- Neo4j 1.1.x
