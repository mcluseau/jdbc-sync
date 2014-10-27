jdbc-sync
=========

Published under the terms of the [GNU LGPL](http://www.gnu.org/licenses/lgpl.txt).

Usage (pure JDBC)
-----------------

Not supported yet but planned.

Usage (with Cayenne)
--------------------

Assuming you have another project containing your Cayenne domain, your pom.xml will look like this:

    <project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
        <modelVersion>4.0.0</modelVersion>
        <groupId>nc.isi.project-x</groupId>
        <artifactId>project-x-jobs</artifactId>
        <version>1.0.0</version>
        <dependencies>
            <dependency>
                <groupId>nc.isi.project-x</groupId>
                <artifactId>project-x-model</artifactId>
                <version>1.0.0</version>
            </dependency>
    
            <!-- jdbc-sync -->
            <dependency>
                <groupId>nc.isi</groupId>
                <artifactId>jdbc-sync</artifactId>
                <version>0.0.3</version>
            </dependency>
    
            <!-- JDBC drivers, here DB2/AS400 and PostgreSQL 9.3 -->
            <dependency>
                <groupId>org.postgresql</groupId>
                <artifactId>postgresql</artifactId>
                <version>9.3-1100-jdbc41</version>
            </dependency>
            <dependency>
                <groupId>net.sf.jt400</groupId>
                <artifactId>jt400</artifactId>
                <version>6.7</version>
            </dependency>
        </dependencies>
        <build>
            <plugins>
                <plugin>
                    <artifactId>maven-assembly-plugin</artifactId>
                    <version>2.4.1</version>
                    <configuration>
                        <descriptorRefs>
                            <descriptorRef>jar-with-dependencies</descriptorRef>
                        </descriptorRefs>
                    </configuration>
                </plugin>
            </plugins>
        </build>
    </project>

This way, you can build a standalone jar and run any table migration this way:

    mvn assembly:single
    java -cp target/project-x-jobs-1.0.0-jar-with-dependencies.jar nc.isi.jdbc_sync.SyncTable data-sources.properties source.table target.table

