/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.sa.cat.ldapjdbcclient;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 *
 * @author vsingh
 */
public class LdapsJdbcClient {

    private static final String CONNECTION_URL_PROPERTY = "connection.url";
	private static final String JDBC_DRIVER_NAME_PROPERTY = "jdbc.driver.class.name";
        private static final String CONNECTION_USERNAME = "connection.username";
        private static final String CONNECTION_PASSWORD = "connection.password";
        private static final String CONNECTION_QUERY = "connection.query";
        private static final String CONNECTION_TRUSTSTORE_FILE = "connection.truststore.jks.file";
        private static final String CONNECTION_TRUSTSTORE_PASSWORD = "connection.truststore.jks.password";

	private static String connectionUrl;
        private static String userName;
        private static String passWord;
	private static String jdbcDriverName;
        private static String userQuery;
        private static String trustStoreFile;
        private static String trustStorePwd;

        private static void loadConfiguration() throws IOException {
                InputStream input = null;
                try {
                        String filename = LdapsJdbcClient.class.getSimpleName() + ".conf.properties";
                        input = LdapsJdbcClient.class.getClassLoader().getResourceAsStream(filename);
                        Properties prop = new Properties();
                        prop.load(input);
        
                        connectionUrl = prop.getProperty(CONNECTION_URL_PROPERTY);
                        jdbcDriverName = prop.getProperty(JDBC_DRIVER_NAME_PROPERTY);
                        userName = prop.getProperty(CONNECTION_USERNAME);
                        passWord = prop.getProperty(CONNECTION_PASSWORD);
                        userQuery = prop.getProperty(CONNECTION_QUERY);
                        trustStoreFile = prop.getProperty(CONNECTION_TRUSTSTORE_FILE);
                        trustStorePwd = prop.getProperty(CONNECTION_TRUSTSTORE_PASSWORD);
                } finally {
                        try {
                                if (input != null)
                                        input.close();
                        } catch (IOException e) {
                                // nothing to do
                        }
                }
        }

	public static void main(String[] args) throws IOException {

                
                loadConfiguration();

                System.setProperty("javax.net.ssl.trustStore", trustStoreFile);
                System.setProperty("javax.net.ssl.trustStorePassword", trustStorePwd);
                String sqlStatement = userQuery;

		System.out.println("\n=============================================");
		System.out.println("Cloudera Impala LDAP JDBC:");
		System.out.println("Using Connection URL: " + connectionUrl);
		System.out.println("Running Query: " + sqlStatement);
		Connection con = null;

		try {

			Class.forName(jdbcDriverName);

			con = DriverManager.getConnection(connectionUrl,userName, passWord);

			Statement stmt = con.createStatement();

			ResultSet rs = stmt.executeQuery(sqlStatement);

			System.out.println("\n== Begin Query Results ======================");

			// print the results to the console
			while (rs.next()) {
				// the example query returns one String column
				System.out.println(rs.getString(1));
			}

			System.out.println("== End Query Results =======================\n\n");

		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}
	}
    
}
