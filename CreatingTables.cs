using System;
using System.Data;
using System.IO;
using Microsoft.Data.SqlClient;
using Serilog;

namespace TableCreation
{//test
    class CreatingTables
    {//sql part a
        private const string masterConnectionString = "Server=localhost,1433;User Id=sa;Password=YourStrong\\!Password;TrustServerCertificate=True;";
        private const string databaseConnectionString = "Server=localhost,1433;Database=SchoolDB;User Id=sa;Password=YourStrong\\!Password;TrustServerCertificate=True;";

        public static void GenerateSQLTasks()
        {
            // Define file paths
            string outputFilePath = "CreatingTablesOutput.log";
            string errorLogFilePath = "CreatingTablesErrors.log";

            // Initialize Serilog with dynamic file paths
            Log.Logger = Logger.CreateLogger(outputFilePath, errorLogFilePath);

            try
            {
                Log.Information("Starting the application...");

                // Call the function to create the database, tables, and insert sample data
                CreateDatabaseAndTables();
                ExecuteSQLQueries();

                Log.Information("Application completed successfully.");
            }
            catch (Exception ex)
            {
                Log.Error(ex, "An unhandled exception occurred.");
            }
            finally
            {
                Log.CloseAndFlush(); // Ensure logs are flushed to files
            }
        }

        static void CreateDatabaseAndTables()
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(masterConnectionString))
                {
                    connection.Open();
                    var createDbCommand = new SqlCommand("IF DB_ID('SchoolDB') IS NULL CREATE DATABASE SchoolDB;", connection);
                    createDbCommand.ExecuteNonQuery();
                    Log.Information("Database 'SchoolDB' checked/created successfully.");
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error creating database.");
            }

            using (SqlConnection connection = new SqlConnection(databaseConnectionString))
            {
                connection.Open();

                string createStudentsTable = @"
                    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Students' AND xtype='U')
                    CREATE TABLE Students (
                        StudentID INT IDENTITY(1,1) PRIMARY KEY,
                        StudentFirstName NVARCHAR(50),
                        StudentLastName NVARCHAR(50)
                    );";
                ExecuteNonQueryCommand(createStudentsTable, connection);

                string createTestsTable = @"
                    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Tests' AND xtype='U')
                    CREATE TABLE Tests (
                        TestID INT IDENTITY(1,1),
                        TestDate DATE,
                        ClassName NVARCHAR(50),
                        StudentID INT,
                        Grade FLOAT,
                        PRIMARY KEY (TestID, ClassName),
                        FOREIGN KEY (StudentID) REFERENCES Students(StudentID)
                    );";
                ExecuteNonQueryCommand(createTestsTable, connection);

                string insertStudents = @"
                    INSERT INTO Students (StudentFirstName, StudentLastName)
                    VALUES 
                    ('Ofek', 'Dinisman'),
                    ('Bobi', 'Levi'),
                    ('Meny', 'Ozery');";
                ExecuteNonQueryCommand(insertStudents, connection);

                string insertTests = @"
                    INSERT INTO Tests (TestDate, ClassName, StudentID, Grade)
                    VALUES 
                    ('2024-11-11', 'Math', 1, 85),
                    ('2024-11-11', 'History', 1, 90),
                    ('2024-11-11', 'Math', 2, 75),
                    ('2024-11-11', 'History', 2, 80),
                    ('2024-11-11', 'Math', 3, 95),
                    ('2024-11-11', 'History', 3, 85);";
                ExecuteNonQueryCommand(insertTests, connection);
            }

            Log.Information("Database setup operations completed successfully.");
        }

        static void ExecuteNonQueryCommand(string query, SqlConnection connection)
        {
            try
            {
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    int rowsAffected = command.ExecuteNonQuery();
                    Log.Information("Executed Non-Query: {Query}", query);
                    Log.Information("Rows Affected: {RowsAffected}", rowsAffected);
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error executing query: {Query}", query);
            }
        }

        static void ExecuteSQLQueries()
        {

            using (var connection = new SqlConnection(databaseConnectionString))
            {
                connection.Open();

                ExecuteQueryToCsv(
                    @"
                    SELECT s.StudentFirstName, s.StudentLastName, t.ClassName, AVG(t.Grade) AS AverageGrade
                    FROM Students s
                    JOIN Tests t ON s.StudentID = t.StudentID
                    GROUP BY s.StudentFirstName, s.StudentLastName, t.ClassName
                    ORDER BY s.StudentFirstName, s.StudentLastName, t.ClassName;",
                    connection, "AverageGradesByClass.csv");

                ExecuteQueryToCsv(
                    @"
                    SELECT s.StudentFirstName, s.StudentLastName, AVG(t.Grade) AS OverallAverage
                    FROM Students s
                    JOIN Tests t ON s.StudentID = t.StudentID
                    GROUP BY s.StudentFirstName, s.StudentLastName
                    ORDER BY s.StudentFirstName, s.StudentLastName;",
                    connection, "OverallAverageGrades.csv");

                ExecuteQueryToCsv(
                    @"
                    WITH SubjectAvg AS (
                        SELECT ClassName, AVG(Grade) AS SubjectAverage
                        FROM Tests
                        GROUP BY ClassName
                    ), OverallAvg AS (
                        SELECT AVG(Grade) AS OverallAverage
                        FROM Tests
                    )
                    SELECT sa.ClassName, sa.SubjectAverage, oa.OverallAverage, 
                           (sa.SubjectAverage - oa.OverallAverage) AS Difference, 
                           ((sa.SubjectAverage - oa.OverallAverage) / oa.OverallAverage) * 100 AS DeviationPercentage
                    FROM SubjectAvg sa, OverallAvg oa
                    ORDER BY sa.ClassName;",
                    connection, "SubjectAverageComparison.csv");
            }
        }

        static void ExecuteQueryToCsv(string query, SqlConnection connection, string filePath)
        {
            try
            {
                using (SqlCommand command = new SqlCommand(query, connection))
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    using (StreamWriter writer = new StreamWriter(filePath))
                    {
                        // Write column headers
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            writer.Write(reader.GetName(i));
                            if (i < reader.FieldCount - 1)
                                writer.Write(",");
                        }
                        writer.WriteLine();

                        // Write rows
                        while (reader.Read())
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                writer.Write(reader[i].ToString());
                                if (i < reader.FieldCount - 1)
                                    writer.Write(",");
                            }
                            writer.WriteLine();
                        }
                    }
                    Log.Information("Query results saved to file: {FilePath}", filePath);
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error saving query results to file: {FilePath}", filePath);
            }
        }
    }
}
