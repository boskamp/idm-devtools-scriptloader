package org.idmacs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Base64;

public class IDMCLI {
	private static boolean g_verbose;

	private static void trc(String msg) {
		if (g_verbose) {
			System.err.println(msg);
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

		if (scriptBase64String.startsWith("{B64}")) {
			scriptBase64String = scriptBase64String.substring("{B64}".length());
		}

		byte[] scriptUtf8Bytes = Base64.decodeBase64(scriptBase64String);
		os.write(scriptUtf8Bytes, 0, scriptUtf8Bytes.length);
		os.flush();
		os.close();
	}

	private static void doDownload(String jdbcUrl, List<String> fileNames) throws Exception {
		final String M = "doDownload: ";
		trc(M + "Entering jdbcUrl=" + jdbcUrl+", fileNames="+fileNames);

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

	private static void doUpload(String jdbcUrl, List<String> fileNames) throws Exception {
		System.err.println("Not implemented yet");
	}

	public static void main(String[] args) throws Exception {
		final String M = "main: ";
		trc(M + "Entering args=" + args);

		// create the command line parser
		CommandLineParser parser = new DefaultParser();

		// create the Options
		Options options = new Options();

		options.addOption(Option //
				.builder("u") // short name
				.longOpt("url") // long name
				.required() //
				.desc("JDBC URL for _admin connection") // description
				.hasArg() //
				.argName("JDBC_URL") //
				.build());

		options.addOption(Option //
				.builder("v") // short name
				.longOpt("verbose") // long name
				.required(false) //
				.desc("Output trace messages to STDERR") // description
				.build());

		options.addOption(Option //
				.builder("h") // short name
				.longOpt("help") // long name
				.required(false) //
				.desc("Display this help") // description
				.build());

		boolean exceptionOccurred = false;
		CommandLine line = null;
		try {

			// parse the command line arguments
			line = parser.parse(options, args);
			String jdbcUrl = line.getOptionValue('u');

			g_verbose = line.hasOption('v');

			List<String> aCmd = line.getArgList();
			if (aCmd.size() > 0) {
				String cmd = aCmd.get(0).toLowerCase();
				List<String> aFileNames = aCmd.subList(1, aCmd.size());
				if ("get".startsWith(cmd)) {
					doDownload(jdbcUrl, aFileNames);
				} else if ("put".startsWith(cmd)) {
					doUpload(jdbcUrl, aFileNames);
				} else {
					throw new Exception("Unrecognized command: " + cmd);
				}
			}
			else {
				throw new Exception("Missing command");
			}

		} catch (Exception e) {
			System.err.println(e.getMessage());
			exceptionOccurred = true;
		}

		if (exceptionOccurred || (line != null && line.hasOption('h'))) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.setLeftPadding(4);
			String br = formatter.getNewLine();
			char[] alp = new char[formatter.getLeftPadding()];
			Arrays.fill(alp, ' ');
			String lp = new String(alp);
			String header = "Download or upload package scripts from SAP IDM database" + br //
					+ br //
					+ "COMMANDS:"+br //
					+ lp + "get: download from database to filesystem" + br // 
					+ lp + "put: upload from filesystem to database" + br //
					+ br //
					+ "OPTIONS:"+br //
					;
			
			String footer = br // 
					+"EXAMPLES:"+br //
					+ "idmcli get de.foxysoft.core -u jdbc:sqlserver://..."+br //
					+ lp + "Download all scripts from package de.foxysoft.core"+br //
					+ lp + "into local directory $(pwd)/de.foxysoft.core, which will"+br //
					+ lp + "be created if necessary."+br //
					+ br //
					+ "idmcli put de.foxsoft.core -u jdbc:sqlserver://..."+br //
					+ lp + "Upload all *.js files located in directory $(pwd)/de.foxysoft.core"+br //
					+ lp + "into package de.foxysoft.core in the database"+br //
					+ br //
					+ "idmcli put de.foxysoft.core/fx_trace.js     \\"+br //
					+ "           de.foxysoft.core/fx_db_nolock.js \\"+br //
					+ "        -u jdbc:sqlserver://..."+br //
					+ lp + "Upload the two files fx_trace.js and fx_db_nolock.js,"+br //
					+ lp + "stored in local directory $(pwd)/de.foxysoft.core,"+br //
					+ lp + "into package de.foxysoft.core in the database"+br //
					;

			//Output options in the order they have been declared
			formatter.setOptionComparator(null);
			
			formatter.printHelp("idmcli <cmd> <package>[/<script>]...", header, options, footer, true);
		}

		trc(M + "Returning");
	}// main

}// IDMCLI
