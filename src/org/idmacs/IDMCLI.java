package org.idmacs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.binary.Base64;

public class IDMCLI {
	private static boolean g_verbose;

	private static void trc(String msg) {
		if (g_verbose) {
			System.err.println(msg);
		}
	}

	private static void registerJdbcDrivers() {
		try {
			Class.forName("com.microsoft.jdbc.sqlserver.SQLServerDriver");
		} catch (ClassNotFoundException e) {
		}
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
		}
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
		}
		try {
			Class.forName("com.ibm.db2.jcc.DB2Driver");
		} catch (ClassNotFoundException e) {
		}
	}

	private static void writeScriptToFile(ResultSet rs) throws Exception {
		String pkgDirName = rs.getString(1);
		String scriptName = rs.getString(2);
		File pkgDirFile = new File(pkgDirName);
		File scriptFile = null;

		if (!pkgDirFile.exists()) {
			if (!pkgDirFile.mkdir()) {
				throw new Exception("Failed to create directory "
						+ pkgDirFile.getAbsolutePath());
			}
		} else {
			if (!pkgDirFile.isDirectory()) {
				throw new Exception("Cannot create directory " + pkgDirName
						+ "because file " + pkgDirFile.getAbsolutePath()
						+ "exists");
			}
		}

		scriptFile = new File(pkgDirFile, scriptName + ".js");

		OutputStream os = new FileOutputStream(scriptFile);
		String scriptBase64String = rs.getString(3);

		if (scriptBase64String.startsWith("{b64}") || scriptBase64String.startsWith("{B64}")) {
			scriptBase64String = scriptBase64String.substring("{b64}".length());
		}

		byte[] scriptUtf8Bytes = Base64.decodeBase64(scriptBase64String);
		os.write(scriptUtf8Bytes, 0, scriptUtf8Bytes.length);
		os.flush();
		os.close();
	}

	private static void readScriptsFromDatabase(String jdbcUrl)
			throws Exception {
		final String M = "readScriptsFromDatabase: ";
		trc(M + "Entering jdbcUrl=" + jdbcUrl);

		Connection c = null;
		Statement s = null;
		ResultSet rs = null;
		try {
			c = DriverManager.getConnection(jdbcUrl);
			s = c.createStatement();
			rs = s.executeQuery("select p.mcqualifiedname" // 1
					+ "    ,s.mcscriptname" // 2
					+ "    ,s.mcscriptdefinition" // 3
					+ "    from mc_package_scripts s"
					+ "    inner join mc_package p"
					+ "    on s.mcpackageid=p.mcpackageid"
					+ "    order by"
					+ "    p.mcqualifiedname" // 1
					+ "    ,s.mcscriptname" // 2
			);

			while (rs.next()) {
				writeScriptToFile(rs);
			}
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (s != null) {
					s.close();
				}
				if (c != null) {
					c.close();
				}
			} catch (Exception inner) {
				inner.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		// create the command line parser
		CommandLineParser parser = new DefaultParser();

		// create the Options
		Options options = new Options();

		options.addOption(Option //
				.builder("u") // short name
				.longOpt("url") // long name
				.required() //
				.desc("JDBC URL for _admin connection") // description
				.hasArg().argName("JDBC_URL").build());

		options.addOption(Option //
				.builder("v") // short name
				.longOpt("verbose") // long name
				.required(false) //
				.desc("Output trace messages to STDERR") // description
				.build());
		try {

			// parse the command line arguments
			CommandLine line = parser.parse(options, args);
			String jdbcUrl = line.getOptionValue('u');

			g_verbose = line.hasOption('v');

			registerJdbcDrivers();
			readScriptsFromDatabase(jdbcUrl);

		} catch (ParseException ex) {
			System.err.println(ex.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("idmcli", options);
		}
	}

}
