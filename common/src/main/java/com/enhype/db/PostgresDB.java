package com.enhype.db;

import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.log4j.Logger;

import com.enhype.utils.RunConfig;

public class PostgresDB {
	
	private Logger logger = Logger.getLogger(PostgresDB.class.getName());

	private String DBurl = RunConfig.POSTGRES_CONNECT_STRING;
	private String DBuser = RunConfig.POSTGRES_USER;
	private String DBpassword = RunConfig.POSTGRES_PSW;

	public void closeResultSet(ResultSet result) {

		try {

			Statement st = result.getStatement();

			if (result != null) {
				result.close();
			}

			if (st != null) {
				st.close();
			}

		} catch (SQLException ex) {

			logger.error(ex.getMessage());
			logger.error("SQL Exception: ", ex);

		}

	}

	void closeConnection(Connection con) {

		try {

			if (con != null) {
				con.close();
			}

		} catch (SQLException ex) {

			logger.error(ex.getMessage());
			logger.error("SQL Exception: ", ex);

		}

	}

	void closeStatement(Statement st) {

		try {

			if (st != null) {
				st.close();
			}

		} catch (SQLException ex) {

			logger.error(ex.getMessage());
			logger.error("SQL Exception: ", ex);

		}

	}

	public ResultSet execSelect(String queryString) {

		Connection con = getConnection();
		Statement st = null;
		ResultSet result = null;

		try {

			st = con.createStatement();
			result = st.executeQuery(queryString);

			if (result.isBeforeFirst()) {
				logger.debug("Retrieve successfully from " + DBurl);
				logger.debug("SQL Query: " + queryString);
			}

			return result;

		} catch (SQLException ex) {
			logger.error(queryString);
			logger.error(ex.getMessage());
			logger.error("SQL Exception: ", ex);

		} finally {
			closeConnection(con);
		}
		return null;
	}
	
	public ResultSet execSelect(String queryString, int fetchSize) {

		Connection con = getConnection();
		Statement st = null;
		ResultSet result = null;

		try {

			st = con.createStatement();
			con.setAutoCommit(false);
			st.setFetchSize(fetchSize);
			result = st.executeQuery(queryString);

			if (result.isBeforeFirst()) {
				logger.debug("Retrieve successfully from " + DBurl);
				logger.debug("SQL Query: " + queryString);
			}

			return result;

		} catch (SQLException ex) {
			logger.error(queryString);
			logger.error(ex.getMessage());
			logger.error("SQL Exception: ", ex);

		}
		return null;
	}

	/**
	 * Execute a SQL script No ResultSet return!
	 * 
	 * @param reader
	 */
	public void execScript(Reader reader) {

		Connection con = getConnection();

		try {

			ScriptRunner runner = new ScriptRunner(con);
			runner.runScript(reader);

		} finally {
			closeConnection(con);
		}
	}

	public int execUpdate(String updateString) {

		logger.debug(updateString);
		
		Connection con = getConnection();
		Statement st = null;
		int result = -1;

		try {

			st = con.createStatement();
			result = st.executeUpdate(updateString);

			if (result >= 0) {
				logger.debug("Update successful to " + DBurl);
				logger.debug(updateString);
			}
			return result;

		} catch (SQLException ex) {
			logger.error(updateString);
			logger.error(ex.getMessage());
			logger.error("SQL Exception: ", ex);

		} finally {
			closeConnection(con);
			closeStatement(st);
		}
		return result;
	}

	private Connection getConnection() {
		// TODO Auto-generated method stub
		Connection con = null;
		try {

			con = DriverManager.getConnection(DBurl, DBuser, DBpassword);
			logger.debug("Connected successfully to " + DBurl);

			return con;

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}
	
}
