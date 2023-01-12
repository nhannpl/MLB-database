
build: Project.class

Project.class: Project.java
	javac Project.java

run: Project.class
	java -cp .:mssql-jdbc-11.2.0.jre18.jar Project

clean:
	rm Project.class MLBDatabase.class
