The web service consists of three Java classes.

DataProvider that accepts requests in a Apache/Tomcat/Axis2 environment,
checks parameters and starts up the next class ...

DataProviderSender that handles the requests and send data back over a
network stream to a predefined server.

**** EDIT MATHIEU 2019-08-29 *****
DataProviderSender has been changed to fix several bugs (Timestamtz in the object iw_w2_animaltrack. Fixed by changing the script used for the creation of the table)
Several request exist on WRAM's side, only one is implemented in the WS. To avoid returning an error when a request is not implemented, an empty file is returned.
To change a file and recompile it, I used the directory compile-jar. With the correct tree hierarchy in it.
javac se/lu/canmnove/ws/DataProviderSender.java
jar -cvf DataProviderSender.jar se/lu/canmnove/ws/DataProviderSender.class
and then copy it in /home/sys/canmove/ws
The back-ups of Mats's latest version is in bkp/  (jar and java file)
To test it, get connected to https://wcp.test.wram.eu/ and create a query (CAnMove data provider, raw data view with animal data)
**** END EDIT *****

DataProviderListener is just used for testing. If the maxinmum number of
data rows requested is negative the DataProviderSender will send the data
to localhost instead of the predefined server. The listener should then be
started manually beforehand.

There are also tree configuration files with the same name as the Java class
but with an xml suffix. They are all read as Java objects so thread carefully
when editing. The parameters and configuration files are documented below.

DataProvider
------------
Parameters:
- String[] query: array of five strings with SQL query description
  query[0]: optional maximum number of rows to fetch
  query[1]: optional list of columns to use in select clause
  query[2]: database object to select from
  query[3]: optional selection criteria to use in where clause
  query[4]: optional list of columns to use in order by clause
  It is also possible to supply all strings in query[0] separated by "/"
- String userId: user id to use for authorization
- int portNr: port number to use when streaming data back to receiving server
- int timeout: maximum execution time
The use of userId for authorization is not actually implemented. Instead
there is a flag in the database that either allows access to all data in
a dataset to anyone or restricts access totally.
Maximum execution time is not implemented either.

Configuration in DataProvider.xml:
- logRequest: true/false, should requests be logged to DataProvider.log?
- logDetails: true/false, should be logged in detail to sub catalog "log"?
- classPath: path to DataProviderSender and PostgreSQL driver
- className: full DataProviderSender class name

DataProviderSender
------------------
Parameters: the same parameters as for DataProvider but the first parameter
            must be in the form of a single string as described above

Configuration in DataProviderSender.xml:
- logRequest: true/false, should requests be logged to DataProviderSender.log?
- logDetails: true/false, should be logged in detail to sub catalog "log"?
- IPHostName: predefined server to send data back to
- DBDriver: full PostgreSQL class name
- DBString: PostgreSQL connect string
- DBUser: PostgreSQL user name
- DBPassword: PostgreSQL password

DataProviderListener
--------------------
Parameters:
- int portNr: port number to listen on
- int timeout: maximum execution time
Maximum execution time is not implemented.

Configuration in DataProviderListener.xml:
- logRequest: true/false, should requests be logged to DataProviderListener.log?
- logDetails: true/false, should be logged in detail to sub catalog "log"?
