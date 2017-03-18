package saq.create.load.table.FY1415;
//  Can not issue data manipulation statements with executeQuery().

//STEP 1. Import required packages
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

//TODO include categories.h;

public class CreateLoadTableFY1415 {

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://192.168.178.235:3306/SaQ?autoReconnect=true&useSSL=false";
	static final String USER = "geoff2";
	static final String PASS = "talk22me";
	static String ejLineTablename    = "EJLine";
	static String ejTxnTablename     = "EJTxn";
	static String ejFile001Tablename = "File001";
	static String ejFile002Tablename = "File002";
	static String ejFile005Tablename = "File005";
	static String ejFile020Tablename = "File020";
static String sql = null;
	private static String prevDate = "";
	private static int ID = 0;
	private static int lineCount = 0;
	private static int txnCount = 0;
	private static String[] category = new String[201];
	static Connection conn;
	static Statement stmt;

	public static void main(String[] args) {

		initialiseCategories();

		try {
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("Connecting to database...");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);

//			createEJLineTable();
//			createEJTxnTable();
			insertToTables();

			conn.close();
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			} // nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			} // end finally try
		} // end try
		System.out.println("line count = " + lineCount);
		System.out.println("txn  count = " + txnCount);
		System.out.println("Goodbye!");
	}// end
		// main

	private static void initialiseCategories() {
		category[1] = "Fabric      ";
		category[2] = "Workshop    ";
		category[3] = "Sale Fabric ";
		category[4] = "GiftCert    ";
		category[5] = "Dept005     ";
		category[6] = "Notion      ";
		category[7] = "BOM         ";
		category[8] = "Threads     ";
		category[9] = "Dept009     ";
		category[10] = "DEPT010     ";
		category[11] = "Book        ";
		category[12] = "Postage     ";
		category[13] = "Kits        ";
		category[14] = "DEPT014     ";
		category[15] = "DEPT015     ";
		category[16] = "Class       ";
		category[17] = "Long Arm    ";
		category[18] = "Patterns    ";
		category[19] = "DEPT019     ";
		category[20] = "Pattern     ";
		category[21] = "Janome      ";
		category[22] = "Batting     ";
		category[23] = "Misc.       ";
		category[24] = "DEPT024     ";
		category[25] = "Dept025     ";
		category[26] = "DEPT026     ";
		category[27] = "DEPT027     ";
		category[28] = "DEPT028     ";
		category[29] = "DEPT029     ";
		category[30] = "DEPT030     ";
		category[31] = "DEPT031     ";
		category[32] = "DEPT032     ";
		category[33] = "DEPT033     ";
		category[34] = "DEPT034     ";
		category[35] = "DEPT035     ";
		category[36] = "DEPT036     ";
		category[37] = "DEPT037     ";
		category[38] = "DEPT038     ";
		category[39] = "DEPT039     ";
		category[40] = "DEPT040     ";
		category[41] = "DEPT041     ";
		category[42] = "DEPT042     ";
		category[43] = "DEPT043     ";
		category[44] = "DEPT044     ";
		category[45] = "DEPT045     ";
		category[46] = "DEPT046     ";
		category[47] = "DEPT047     ";
		category[48] = "DEPT048     ";
		category[49] = "DEPT049     ";
		category[50] = "DEPT050     ";
		category[51] = "DEPT051     ";
		category[52] = "DEPT052     ";
		category[53] = "DEPT053     ";
		category[54] = "DEPT054     ";
		category[55] = "DEPT055     ";
		category[56] = "DEPT056     ";
		category[57] = "DEPT057     ";
		category[58] = "DEPT058     ";
		category[59] = "DEPT059     ";
		category[60] = "DEPT060     ";
		category[61] = "DEPT061     ";
		category[62] = "DEPT062     ";
		category[63] = "DEPT063     ";
		category[64] = "DEPT064     ";
		category[65] = "DEPT065     ";
		category[66] = "DEPT066     ";
		category[67] = "DEPT067     ";
		category[68] = "DEPT068     ";
		category[69] = "DEPT069     ";
		category[70] = "DEPT070     ";
		category[71] = "DEPT071     ";
		category[72] = "DEPT072     ";
		category[73] = "DEPT073     ";
		category[74] = "DEPT074     ";
		category[75] = "DEPT075     ";
		category[76] = "DEPT076     ";
		category[77] = "DEPT077     ";
		category[78] = "DEPT078     ";
		category[79] = "DEPT079     ";
		category[80] = "DEPT080     ";
		category[81] = "DEPT081     ";
		category[82] = "DEPT082     ";
		category[83] = "DEPT083     ";
		category[84] = "DEPT084     ";
		category[85] = "DEPT085     ";
		category[86] = "DEPT086     ";
		category[87] = "DEPT087     ";
		category[88] = "DEPT088     ";
		category[89] = "DEPT089     ";
		category[90] = "DEPT090     ";
		category[91] = "DEPT091     ";
		category[92] = "DEPT092     ";
		category[93] = "DEPT093     ";
		category[94] = "DEPT094     ";
		category[95] = "DEPT095     ";
		category[96] = "DEPT096     ";
		category[97] = "DEPT097     ";
		category[98] = "DEPT098     ";
		category[99] = "DEPT099     ";
		category[100] = "DEPT100     ";
		category[101] = "DEPT101     ";
		category[102] = "DEPT102     ";
		category[103] = "DEPT103     ";
		category[104] = "DEPT104     ";
		category[105] = "DEPT105     ";
		category[106] = "DEPT106     ";
		category[107] = "DEPT107     ";
		category[108] = "DEPT108     ";
		category[109] = "DEPT109     ";
		category[110] = "DEPT110     ";
		category[111] = "DEPT111     ";
		category[112] = "DEPT112     ";
		category[113] = "DEPT113     ";
		category[114] = "DEPT114     ";
		category[115] = "DEPT115     ";
		category[116] = "DEPT116     ";
		category[117] = "DEPT117     ";
		category[118] = "DEPT118     ";
		category[119] = "DEPT119     ";
		category[120] = "DEPT120     ";
		category[121] = "DEPT121     ";
		category[122] = "DEPT122     ";
		category[123] = "DEPT123     ";
		category[124] = "DEPT124     ";
		category[125] = "DEPT125     ";
		category[126] = "DEPT126     ";
		category[127] = "DEPT127     ";
		category[128] = "DEPT128     ";
		category[129] = "DEPT129     ";
		category[130] = "DEPT130     ";
		category[131] = "DEPT131     ";
		category[132] = "DEPT132     ";
		category[133] = "DEPT133     ";
		category[134] = "DEPT134     ";
		category[135] = "DEPT135     ";
		category[136] = "DEPT136     ";
		category[137] = "DEPT137     ";
		category[138] = "DEPT138     ";
		category[139] = "DEPT139     ";
		category[140] = "DEPT140     ";
		category[141] = "DEPT141     ";
		category[142] = "DEPT142     ";
		category[143] = "DEPT143     ";
		category[144] = "DEPT144     ";
		category[145] = "DEPT145     ";
		category[146] = "DEPT146     ";
		category[147] = "DEPT147     ";
		category[148] = "DEPT148     ";
		category[149] = "DEPT149     ";
		category[150] = "DEPT150     ";
		category[151] = "DEPT151     ";
		category[152] = "DEPT152     ";
		category[153] = "DEPT153     ";
		category[154] = "DEPT154     ";
		category[155] = "DEPT155     ";
		category[156] = "DEPT156     ";
		category[157] = "DEPT157     ";
		category[158] = "DEPT158     ";
		category[159] = "DEPT159     ";
		category[160] = "DEPT160     ";
		category[161] = "DEPT161     ";
		category[162] = "DEPT162     ";
		category[163] = "DEPT163     ";
		category[164] = "DEPT164     ";
		category[165] = "DEPT165     ";
		category[166] = "DEPT166     ";
		category[167] = "DEPT167     ";
		category[168] = "DEPT168     ";
		category[169] = "DEPT169     ";
		category[170] = "DEPT170     ";
		category[171] = "DEPT171     ";
		category[172] = "DEPT172     ";
		category[173] = "DEPT173     ";
		category[174] = "DEPT174     ";
		category[175] = "DEPT175     ";
		category[176] = "DEPT176     ";
		category[177] = "DEPT177     ";
		category[178] = "DEPT178     ";
		category[179] = "DEPT179     ";
		category[180] = "DEPT180     ";
		category[181] = "DEPT181     ";
		category[182] = "DEPT182     ";
		category[183] = "DEPT183     ";
		category[184] = "DEPT184     ";
		category[185] = "DEPT185     ";
		category[186] = "DEPT186     ";
		category[187] = "DEPT187     ";
		category[188] = "DEPT188     ";
		category[189] = "DEPT189     ";
		category[190] = "DEPT190     ";
		category[191] = "DEPT191     ";
		category[192] = "DEPT192     ";
		category[193] = "DEPT193     ";
		category[194] = "DEPT194     ";
		category[195] = "DEPT195     ";
		category[196] = "DEPT196     ";
		category[197] = "DEPT197     ";
		category[198] = "DEPT198     ";
		category[199] = "DEPT199     ";
		category[200] = "DEPT200     ";

	}

	private static void insertToTables() {
		// TODO Auto-generated method stub
		// System.out.println("Creating statement...");
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String csvFile = "/Users/geoffn/Desktop/FY 14 15 sales.csv";

		Reader in = null;
		try {
			in = new FileReader(csvFile);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		Iterable<CSVRecord> records = null;
		try {
			records = CSVFormat.EXCEL.parse(in);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		for (CSVRecord record : records) {
			String dateStr = record.get(0);
			DateFormat srcDf = new SimpleDateFormat("dd-MMM-yy");
			// parse the date string into Date object
			try {
				Date date2 = srcDf.parse(dateStr);
				DateFormat destDf = new SimpleDateFormat("yyyy-MM-dd");
				// format the date into another format
				dateStr = destDf.format(date2);
//				System.out.println("Converted date is : " + dateStr);

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			float Income = getFloat(record, 1);
			float Books = getFloat(record, 2);
			float ClassCost = getFloat(record, 3);
			float Janome = getFloat(record, 4);
			float BOTM = getFloat(record, 5);
			float Longarm = getFloat(record, 6);
			float Batting = getFloat(record, 7);
			float FabricsNotions = getFloat(record, 8);
			float Postage = getFloat(record, 9);
			float SummerSchool = getFloat(record, 10);

			writeEJLines(dateStr, Income, Books, ClassCost, Janome, BOTM, Longarm, Batting, FabricsNotions, Postage,
					SummerSchool);
		}
	}

	private static float getFloat(CSVRecord record, int recordno) {
		if (record.get(recordno).isEmpty() || record.get(recordno).contains("-")) {
			return 0;
		} else {
			return Float.parseFloat(record.get(recordno).replaceAll("[,()]", ""));
		}
	}

	private static void writeEJLines(String date, float income, float books, float classCost, float janome, float bOTM,
			float longarm, float batting, float fabricsNotions, float postage, float summerSchool) {
		String LineDateTime = null;
		int LineNumber = 1;
		int Quantity = 1;
		float DiscountPCT = 0;
		float Discount = 0;
		LineDateTime = date;
		if (date.equals(prevDate)) {
			ID++;
		} else {
			ID = 1;
			prevDate = date;
		}
		if (books > 0) {
			insertEJLine(LineDateTime, ID, LineNumber, Quantity, category[11], books, DiscountPCT, Discount);
			LineNumber++;
		}
		if (classCost > 0) {
			insertEJLine(LineDateTime, ID, LineNumber, Quantity, category[16], classCost, DiscountPCT, Discount);
			LineNumber++;
		}
		if (janome > 0) {
			insertEJLine(LineDateTime, ID, LineNumber, Quantity, category[21], janome, DiscountPCT, Discount);
			LineNumber++;
		}
		if (bOTM > 0) {
			insertEJLine(LineDateTime, ID, LineNumber, Quantity, category[7], bOTM, DiscountPCT, Discount);
			LineNumber++;
		}
		if (longarm > 0) {
			insertEJLine(LineDateTime, ID, LineNumber, Quantity, category[17], longarm, DiscountPCT, Discount);
			LineNumber++;
		}
		if (batting > 0) {
			insertEJLine(LineDateTime, ID, LineNumber, Quantity, category[22], batting, DiscountPCT, Discount);
			LineNumber++;
		}
		if (fabricsNotions > 0) {
			insertEJLine(LineDateTime, ID, LineNumber, Quantity, category[1], fabricsNotions, DiscountPCT, Discount);
			LineNumber++;
		}
		if (postage > 0) {
			insertEJLine(LineDateTime, ID, LineNumber, Quantity, category[12], postage, DiscountPCT, Discount);
			LineNumber++;
		}
		if (summerSchool > 0) {
			insertEJLine(LineDateTime, ID, LineNumber, Quantity, category[2], summerSchool, DiscountPCT, Discount);
			LineNumber++;
		}
		try {
			insertEJTxn(date, "01:00:00", ID, income, false, false, false, 0, 0, 0);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// ID++;
	}

	private static void insertEJLine(String lineDateTime, int iD, int lineNumber, int quantity, String category,
			float amount, float discountPCT, float discount) {
		// System.out.println("Creating statement...");
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		String sql;


		sql = "insert into " + ejLineTablename + "(" + "LineDateTime, " + "ID, " + "LineNumber, " + "Quantity, "
				+ "Category, " + "Amount, " + "DiscountPCT, " + "Discount" + ") VALUES (" + "'" + lineDateTime + "', "
				+ iD + "," + lineNumber + ", " + quantity + ", '" + category + "', " + amount + ", " + discountPCT
				+ ", " + discount + " " + ");";
//		System.out.println(sql);
		lineCount++;

		int result = 0;
		try {
			result = stmt.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
			
		}
		if (result > 1) {
			System.out.println("Executed insert into " + ejLineTablename + ", result= " + result);
		}
		try {
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	private static void insertEJTxn(String journalTxnDate2, String journalTxnTime2, int journalTxnID2,
			// float journalTxnPayLine2,
			float journalTxnPaymentTotal2, boolean JournalTxnPaymentCHARGE2, boolean JournalTxnPaymentCASH2,
			boolean JournalTxnPaymentCOUPON2, float journalTxnPaymentDiscountPCT2, float journalTxnPaymentDiscount2,
			float JournalTxnPaymentRounding2) throws SQLException {
		txnCount++;
		// System.out.println("Creating statement...");
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		String sql;
		sql = "insert into " + ejTxnTablename + "(" + " PaymentDateTime, " + " PaymentID, " + " PaymentTotal, "
				+ " PaymentIsCHARGE, " + " PaymentIsCASH, " + " PaymentIsCOUPON, " + " PaymentIsUNKNOWN, "
				+ " PaymentDiscountPCT, " + " PaymentDiscount, " + " PaymentRounding " + ") VALUES (" + "'"
				+ journalTxnDate2 + "', " + journalTxnID2 + "," + journalTxnPaymentTotal2 + ","
				+ JournalTxnPaymentCHARGE2 + "," + JournalTxnPaymentCASH2 + "," + JournalTxnPaymentCOUPON2 + "," + false
				+ "," + journalTxnPaymentDiscountPCT2 + "," + journalTxnPaymentDiscount2 + ","
				+ JournalTxnPaymentRounding2 + ");";
//		System.out.println(sql);
		System.out.print("*");
		if ((txnCount % 150) == 1)
			System.out.println(txnCount);

		int result = ((Statement) stmt).executeUpdate(sql);
		if (result > 1) {
			System.out.println("Executed insert into " + ejTxnTablename + ", result= " + result);
		}
		stmt.close();
	}

}