package com.tigerit.exam;

import java.util.Scanner;
import java.sql.*;

/**
 * @author Anika Salsabil
 */
public class Solution {

    //Database name
    static final String DATABASE_NAME = "TIGERIT";

    //  Database credentials
    static final String USER = "root";
    static final String PASS = "";

    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost/" + DATABASE_NAME;

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        Statement stmt1 = null;
        Statement stmt2 = null;

        int nC = 0;
        int nD = 0;
        String[] splitColumns = {};
        String[][] records = null;
        String nQ = "";

        Scanner sc = new Scanner(System.in);
        //System.out.println("Test cases:");
        String T = sc.nextLine();             //no of test cases       

        int no_testcases = Integer.parseInt(T);

        for (int i = 0; i < no_testcases; i++) {
            //System.out.println("No of tables:");
            String nT = sc.nextLine();         //no of tables
            int no_tables = Integer.parseInt(nT);
            String[] tables = new String[no_tables];

            for (int j = 0; j < no_tables; j++) {
                //System.out.println("Table name: ");
                tables[j] = sc.nextLine();   //name of table

                //System.out.println("No of columns and records for table: " + tables[j]);
                String CR = sc.nextLine();
                String[] splitCR = CR.split("\\s+");
                if (splitCR.length == 2) {
                    nC = Integer.parseInt(splitCR[0]);
                    nD = Integer.parseInt(splitCR[1]);
                    //System.out.println("No of columns for table: " + nC);
                    //System.out.println("No of records for table: " + nD);

                    //System.out.println("Name of columns: ");
                    String columns = sc.nextLine();
                    splitColumns = columns.split("\\s+");
                    if (splitColumns.length == nC) {
                        //System.out.println("Column names: ");
                        //System.out.println(splitColumns[k]);
                        records = new String[nD][nC];
                        //System.out.println("Records for table: " + tables[j]);
                        for (int row = 0; row < nD; row++) {
                            //System.out.print("Row " + (row + 1) + " : ");
                            for (int col = 0; col < nC; col++) {
                                records[row][col] = sc.next();
                                //records[row][col] = String.valueOf(sc.next());   //ei line er jonne space die niteparchi record 

                                //System.out.print(records[m][n]+" ");  
                            }
                            sc.nextLine();
                        }

                    } else {
                        System.out.println("Invalid COLUMN entry!");
                    }

                } else {
                    System.out.println("Invalid entry of NO OF COL & RECORDS!");
                }

                try {
                    //Register JDBC driver
                    Class.forName("com.mysql.jdbc.Driver");

                    //Open a connection
                    //System.out.println("Connecting to a selected database...");
                    conn = DriverManager.getConnection(DB_URL, USER, PASS);
                    //System.out.println("Connected database successfully...");

                    //Droping table IF EXISTS
                    //System.out.println("Dropping table in given database(if it exists)...");
                    stmt = conn.createStatement();
                    String sql = "DROP TABLE IF EXISTS " + tables[j];
                    stmt.executeUpdate(sql);
                    //System.out.println("DROPPED TABLE in given database...");

                    //Execute a TABLE CREATE query
                    //System.out.println("Creating table in given database...");
                    stmt1 = conn.createStatement();

                    String MYsql1 = "CREATE TABLE " + tables[j] + " (";
                    String colNameDataType = "";
                    for (int n = 0; n < nC; n++) {
                        colNameDataType = colNameDataType.concat(splitColumns[n] + " INTEGER ");
                        if (n != nC - 1) {
                            colNameDataType = colNameDataType.concat(", ");
                        } else {
                            colNameDataType = colNameDataType.concat(" ,PRIMARY KEY (" + splitColumns[0] + "))");
                        }

                    }
                    String sql1 = MYsql1.concat(colNameDataType);
                    stmt1.executeUpdate(sql1);
                    //System.out.println("CREATED TABLE in given database...");
                    ///////////////////////////////////////////////////////////

                    //Execute a INSERT TABLE query
                    //System.out.println("Inserting records into table...");
                    stmt2 = conn.createStatement();

                    String MYsql2 = null;
                    String sql2 = null;
                    String val = "";

                    for (int row = 0; row < nD; row++) {
                        MYsql2 = "INSERT INTO " + tables[j] + " VALUES(";
                        for (int col = 0; col < nC; col++) {
                            if (col != nC - 1) {
                                val = val.concat(records[row][col] + ", ");
                            } else {
                                val = val.concat(records[row][col]);
                            }
                        }
                        sql2 = MYsql2.concat(val + " )");
                        stmt1.executeUpdate(sql2);
                        //System.out.println("INSERTED RECORDS into table...");
                        val = "";
                    }

                    ///////////////////////////////////////////////////////////
                } catch (SQLException se) {
                    //Handle errors for JDBC
                    se.printStackTrace();
                } catch (Exception e) {
                    //Handle errors for Class.forName
                    e.printStackTrace();
                } finally {
                    //finally block used to close resources
                    try {
                        if (stmt1 != null || stmt2 != null) {
                            conn.close();
                        }
                    } catch (SQLException se) {
                    }// do nothing
                    try {
                        if (conn != null) {
                            conn.close();
                        }
                    } catch (SQLException se) {
                        se.printStackTrace();
                    }//end finally try
                }//end try
                //System.out.println("Goodbye from DB table Making and inserting!");

            }

            //System.out.println("Ok");
            try {
                //STEP 2: Register JDBC driver
                Class.forName("com.mysql.jdbc.Driver");

                //STEP 3: Open a connection
                //System.out.println("Connecting to a selected database...");
                conn = DriverManager.getConnection(DB_URL, USER, PASS);
                //System.out.println("Connected database successfully...");

                //STEP 4: Execute a query
                //System.out.println("Join query executing...");
                stmt = conn.createStatement();

                /**
                 * * ************starting the MAIN QUERY section**********
                 */
                //System.out.println("Enter no of queries: ");
                nQ = sc.nextLine();
                int noOfQueries = Integer.parseInt(nQ);
                int fourLines = 4;
                String[][] qu = new String[noOfQueries][fourLines];//4line er query ek row te thakbe(2D)
                String[] query = new String[1000];
                //String[] query = {};

                //input neyar kaj shuru
                for (int qt = 0; qt < noOfQueries; qt++) {
                    //System.out.println("\nEnter your query in 4 lines: ");
                    String ks = "";
                    for (int d = 0; d < fourLines; d++) {
                        String abc = sc.nextLine();
                        ks = ks.concat(abc + " ");

                        //query[qt] = query[qt].concat(qu[qt][d] + " ");
                        //query[qt] = qu[qt][d] + " ";
                    }
                    //System.out.println("SS" + ks);
                    query[qt] = ks;
                    System.out.println("");
                }

                //output dekhanor kaj shuru
                System.out.println("Test: " + (i + 1));

                for (int qt = 0; qt < noOfQueries; qt++) {
                    //System.out.println("Query: " + qt + "   " + query[qt]);
                    ResultSet rs = stmt.executeQuery(query[qt]);
                    ResultSetMetaData rmd = rs.getMetaData();

                    int NumOfCol = rmd.getColumnCount();
                    String[] joinColNames = new String[NumOfCol];
                    for (int r = 0; r < NumOfCol; r++) {
                        joinColNames[r] = rmd.getColumnName(r + 1);
                    }

                    //System.out.println("No of cols: " + NumOfCol);
                    for (int r = 0; r < NumOfCol; r++) {
                        System.out.print(joinColNames[r] + " ");
                    }
                    System.out.println("");

                    int NumOfRow = 0;
                    if (rs.last()) {
                        NumOfRow = rs.getRow();
                    }
                    rs.beforeFirst();
                    //System.out.println("No of rows: " + NumOfRow);
                    String[][] joinValues = new String[NumOfRow][NumOfCol];
                    int e = -1;
                    while (rs.next()) {
                        e++;
                        for (int f = 0; f < NumOfCol; f++) {
                            joinValues[e][f] = rs.getString(joinColNames[f]);
                            System.out.print(joinValues[e][f] + " ");
                        }
                        System.out.println("");
                    }
                    System.out.println("");
                }

                //System.out.println("Executed successfully!");
                //System.out.println();

            } catch (SQLException se) {
                //Handle errors for JDBC
                se.printStackTrace();
            } catch (Exception e) {
                //Handle errors for Class.forName
                e.printStackTrace();
            } finally {
                //finally block used to close resources
                try {
                    if (stmt != null) {
                        conn.close();
                    }
                } catch (SQLException se) {
                }// do nothing
                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException se) {
                    se.printStackTrace();
                }//end finally try
            }//end try

        }
    }
}

