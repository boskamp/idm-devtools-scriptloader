package org.idmacs;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Base64;

public class IDMCLI {

	private static class ShowMessageOnlyException extends Exception {
		static final long serialVersionUID = 1L;

		public ShowMessageOnlyException(String msg) {
			super(msg);
		}

	}

	private static boolean g_verbose;
	private static boolean g_show_progress;

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

	private static void doGet(String jdbcUrl, String pkgName) throws Exception {
		final String M = "doGet: ";
		trc(M + "Entering jdbcUrl=" + jdbcUrl + ", pkgName=" + pkgName);

		Statement s = null;
		ResultSet rs = null;

		Connection con = DriverManager.getConnection(jdbcUrl);

		try {
			s = con.createStatement();

			// TODO: filter on pkgName if supplied
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
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (s != null) {
				try {
					s.close();
				} catch (Exception e) {
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
				}
			}
		}
	}

	private static void doPut(String jdbcUrl, String pkgName,
			List<String> scriptFileNames) throws Exception {
		final String M = "doPut: ";
		trc(M + "Entering jdbcUrl=" + jdbcUrl + ", pkgName=" + pkgName
				+ ", scriptFileNames=" + scriptFileNames);

		if (pkgName == null || pkgName.equals("")) {
			throw new Exception("Package name is mandatory for put");
		}

		if (pkgName.endsWith(System.getProperty("file.separator"))) {
			pkgName = pkgName.substring(0, pkgName.length() - 1);
			trc(M + "Removed trailing file separator: pkgName=" + pkgName);
		}

		File[] scriptFiles = null;

		// If -f given: check existence of each and use as is
		if (!scriptFileNames.isEmpty()) {
			scriptFiles = new File[scriptFileNames.size()];

			for (int i = 0; i < scriptFileNames.size(); ++i) {
				File scriptFile = new File(scriptFileNames.get(i));
				if (scriptFile.exists()) {
					scriptFiles[i] = scriptFile;
				} else {
					System.err.println("File " + scriptFile.getCanonicalPath()
							+ " not found");
				}
			}
		}
		// Otherwise look for *.js files in subdirectory named like package
		else {
			File pkgNameDir = new File(pkgName);
			if (!pkgNameDir.exists()) {
				throw new Exception("Directory " + pkgNameDir.getAbsolutePath()
						+ " does not exist");
			}

			if (!pkgNameDir.isDirectory()) {
				throw new Exception(pkgNameDir.getAbsolutePath()
						+ " must be a directory; found file instead");
			}

			String[] pkgScriptFileNames = pkgNameDir.list(new FilenameFilter() {

				@Override
				public boolean accept(File dir, String name) {
					return name.toLowerCase().endsWith(".js")
							&& !name.startsWith(".");
				}
			});

			Arrays.sort(pkgScriptFileNames);

			scriptFiles = new File[pkgScriptFileNames.length];
			for (int i = 0; i < pkgScriptFileNames.length; ++i) {
				scriptFiles[i] = new File(pkgNameDir, pkgScriptFileNames[i]);
			}
		}// if

		if (scriptFiles.length > 0) {

			Connection con = DriverManager.getConnection(jdbcUrl);
			con.setAutoCommit(false);

			try {
				int pkgId = dbQueryPackageId(con, pkgName);
				Map<String, Integer> pkgScriptsDb = dbQueryPackageScripts(con,
						pkgName);

				for (int i = 0; i < scriptFiles.length; ++i) {
					File scriptFile = scriptFiles[i];
					String scriptFileName = scriptFile.getName();

					if (g_show_progress) {
						System.out.println(//
								(i + 1) + "/" + scriptFiles.length //
										+ ": " + scriptFileName //
								);
					}
					String scriptName = scriptFileName.substring(//
							0, //
							scriptFileName.length() - ".js".length()//
					);

					byte[] rawScriptContent = readRawFile(scriptFile);

					String encodedScriptContent = "{B64}"
							+ Base64.encodeBase64String(rawScriptContent);

					dbUpsertScript(con, scriptName, encodedScriptContent,
							pkgId, pkgScriptsDb);

				}// for

				con.commit();

			} finally {
				if (con != null) {
					try {
						con.close();
					} catch (Exception e) {
					}
				}
			}// finally
		}// if

	}// doPut

	private static Map<String, Integer> dbQueryPackageScripts(Connection con,
			String pkgName) throws Exception {
		PreparedStatement ps = null;
		ResultSet rs = null;
		Map<String, Integer> results = new HashMap<String, Integer>();

		try {
			ps = con.prepareStatement("select mcscriptname,mcscriptid" //
					+ " from mc_package_scripts s"
					+ " inner join mc_package p"
					+ " on s.mcpackageid=p.mcpackageid" //
					+ " where p.mcqualifiedname=?" // 1
			);
			ps.setString(1, pkgName);

			rs = ps.executeQuery();

			while (rs.next()) {
				results.put(rs.getString(1), rs.getInt(2));
			}
		} // try
		finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
				}
			}
		} // finally

		return results;

	}

	private static int dbQueryPackageId(Connection con, String pkgName)
			throws Exception {
		int pkgId = -1;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = con.prepareStatement("select mcpackageid" //
					+ " from mc_package" //
					+ " where mcqualifiedname=?" // 1
			);
			ps.setString(1, pkgName);

			rs = ps.executeQuery();

			if (rs.next()) {
				pkgId = rs.getInt(1);
			} else {
				throw new Exception("Package " + pkgName
						+ " not found in database");
			}
		} // try
		finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
				}
			}
		} // finally

		return pkgId;
	}

	private static void dbUpsertScript(Connection con, String scriptName,
			String encodedScriptContent, int pkgId,
			Map<String, Integer> pkgScriptsDb) throws Exception {

		if (pkgScriptsDb.containsKey(scriptName)) {
			int scriptId = pkgScriptsDb.get(scriptName);
			dbUpdateScript(con, scriptName, scriptId, encodedScriptContent,
					pkgId);
		} else {
			dbInsertScript(con, scriptName, encodedScriptContent, pkgId);
		}
	}

	private static void dbInsertScript(Connection con, String scriptName,
			String encodedScriptContent, int pkgId) throws Exception {
		PreparedStatement ps = null;
		try {
			ps = con.prepareStatement("insert into mc_package_scripts"
					+ "(mcpackageid" // 1
					+ ",mcscriptname" // 2
					+ ",mcscriptlanguage" // 3
					+ ",mcscriptdefinition" // 4
					+ ",mcenabled" // 5
					+ ",mcprotected" // 6
					+ ",mcscriptstatus)" // 7
					+ "values(?,?,?,?,?,?,?)" //
			);

			ps.setInt(1, pkgId);
			ps.setString(2, scriptName);
			ps.setString(3, "JScript");
			ps.setString(4, encodedScriptContent);
			ps.setInt(5, 1);
			ps.setInt(6, 0);
			ps.setInt(7, 0);

			int updateCount = ps.executeUpdate();

			if (updateCount != 1) {
				throw new Exception("INSERT " + scriptName + " returned "
						+ updateCount + " affected rows (should be 1)");
			}

		}// try
		finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
				}
			}
		} // finally

	}// dbInsertScript

	private static void dbUpdateScript(Connection con, String scriptName,
			int scriptId, String encodedScriptContent, int pkgId)
			throws Exception {
		PreparedStatement ps = null;
		try {
			ps = con.prepareStatement("update mc_package_scripts" //
					+ " set mcscriptdefinition=?" // 1
					+ " where mcscriptid=?"// 2
			);
			ps.setString(1, encodedScriptContent);
			ps.setInt(2, scriptId);

			int updateCount = ps.executeUpdate();

			if (updateCount != 1) {
				throw new Exception("UPDATE " + scriptName + " returned "
						+ updateCount + " affected rows (should be 1)");
			}
		} // try
		finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
				}
			}
		} // finally

	}// dbUpdateScript

	private static byte[] readRawFile(File scriptFile) throws Exception {
		byte[] buffer = new byte[1024];
		InputStream is = new FileInputStream(scriptFile);
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		int len;
		while ((len = is.read(buffer)) != -1) {
			os.write(buffer, 0, len);
		}
		is.close();
		return os.toByteArray();

	}// readRawFile

	private static String getMandatoryParam(CommandLine line, char p)
			throws Exception {
		String v = line.getOptionValue(p);
		if (v == null || v.equals("")) {
			throw new ShowMessageOnlyException("Missing mandatory parameter -"
					+ p + "; try idmcli --help");
		}
		return v;
	}// getMandatoryParam

	public static void main(String[] args) throws Exception {
		final String M = "main: ";
		trc(M + "Entering args=" + args);

		// create the command line parser
		CommandLineParser parser = new DefaultParser();

		// create the Options
		Options options = new Options();

		options.addOption(Option //
				.builder("p") // short name
				.longOpt("package") // long name
				.required(false) //
				.desc("package name") // description
				.hasArg() //
				.argName("PKG_NAME") //
				.build());

		options.addOption(Option //
				.builder("f") // short name
				.longOpt("files") // long name
				.required(false) //
				.desc("script files") // description
				.hasArgs() // multiple values allowed
				.argName("FILES...") //
				.build());

		options.addOption(Option //
				.builder("u") // short name
				.longOpt("url") // long name
				.required(false) //
				.desc("JDBC URL for _admin connection") // description
				.hasArg() //
				.argName("JDBC_URL") //
				.build());

		options.addOption(Option //
				.builder("P") // short name
				.longOpt("progress") // long name
				.required(false) //
				.desc("show progress") // description
				.argName("PKG_NAME") //
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

		CommandLine line = null;

		try {

			// parse the command line arguments
			line = parser.parse(options, args);

			if (!line.hasOption('h')) {

				g_verbose = line.hasOption('v');
				g_show_progress = line.hasOption('P');

				List<String> aCmd = line.getArgList();
				if (aCmd.size() > 0) {
					String cmd = aCmd.get(0).toLowerCase();

					if ("get".startsWith(cmd)) {
						String jdbcUrl = getMandatoryParam(line, 'u');
						String pkgName = getMandatoryParam(line, 'p');

						doGet(jdbcUrl, pkgName);
					} else if ("put".startsWith(cmd)) {
						String jdbcUrl = getMandatoryParam(line, 'u');
						String pkgName = getMandatoryParam(line, 'p');
						List<String> scriptFiles = line.hasOption('f') ? Arrays
								.asList(line.getOptionValues('f'))
								: new ArrayList<String>(0);
						doPut(jdbcUrl, pkgName, scriptFiles);
					} else {
						throw new ShowMessageOnlyException(
								"Unrecognized command: " + cmd
										+ "; try idmcli --help");
					}
				} else {
					throw new ShowMessageOnlyException(
							"Missing command; try idmcli --help");
				}
			}// if(!line.hasOption('h'))
			else {
				HelpFormatter formatter = new HelpFormatter();
				formatter.setLeftPadding(4);
				String br = formatter.getNewLine();
				char[] alp = new char[formatter.getLeftPadding()];
				Arrays.fill(alp, ' ');
				String lp = new String(alp);
				String header = "Put/get package scripts into/from SAP IDM database"
						+ br //
						+ br //
						+ "COMMANDS:" + br //
						+ lp + "get: download from database to filesystem" + br //
						+ lp + "put: upload from filesystem to database" + br //
						+ br //
						+ "OPTIONS:" + br //
				;

				String footer = br //
						+ "EXAMPLES:"
						+ br //
						+ "idmcli get -p de.foxysoft.core -u jdbc:sqlserver://..."
						+ br //
						+ lp
						+ "Download all scripts from package de.foxysoft.core"
						+ br //
						+ lp
						+ "into local directory $(pwd)/de.foxysoft.core, which will"
						+ br //
						+ lp
						+ "be created if necessary."
						+ br //
						+ br //
						+ "idmcli put -p de.foxsoft.core -u jdbc:sqlserver://..."
						+ br //
						+ lp
						+ "Upload all *.js files located in directory $(pwd)/de.foxysoft.core"
						+ br //
						+ lp
						+ "into package de.foxysoft.core in the database"
						+ br //
						+ br //
						+ "idmcli put -p de.foxsoft.core -u jdbc:sqlserver://... \\"
						+ br //
						+ "           -f scripts/js/fx_trace.js"
						+ br //
						+ lp
						+ "Upload file fx_trace.js from subdirectory scripts/js"
						+ br //
						+ lp
						+ "into package de.foxysoft.core in the database"
						+ br //
						+ br //
				;

				// Output options in the order they have been declared
				formatter.setOptionComparator(null);

				formatter.printHelp("idmcli <cmd>", header, options, footer,
						true);
			}// else
		} catch (org.apache.commons.cli.MissingArgumentException mae) {
			System.err.println(mae.getMessage() + "; try idmcli --help");
		} catch (ShowMessageOnlyException smoe) {
			System.err.println(smoe.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
		}

		trc(M + "Returning");
	}// main

}// IDMCLI
