// 1div0
// 2020.03.30

package main

// Раздел импорта
import (
    "fmt" // пакет для форматированного ввода вывода
    "os"
    "strings"
    "strconv"
    "encoding/json"
)

type SqlGoForCollect struct {
    Language string
    Version string
}

type TableDescription struct {
    Scheme string
    Table string
    OneRow string
    UseHistory bool
    UseHierarhy bool
    UseProjectId bool
    UseUserId bool
    Cols []ColumnDescription
}

type ColumnDescription struct {
    Name string
    Type string
    Nullable bool
}

type Configuration struct {
    SqlGoFor SqlGoForCollect
    Tables []TableDescription
}

func main() {

    var cfg Configuration

    output_sql_file_name := "output.sql"

    fmt.Println("SQLGO, v1.0")

    if len(os.Args) < 2 {
        fmt.Println("USAGE:")
        fmt.Println("sqlgo.exe <file_name.sqlgo>")
        return
    }

    file_name := os.Args[1]
    file, err := os.Open(file_name)
    if (err != nil) {
        fmt.Println("Error load sqlgo-file: ", err)
        return
    }

    decoder := json.NewDecoder(file)
    err = decoder.Decode(&cfg)
    if (err != nil) {
        fmt.Println("Error decode sqlgo-file: ", err)
        return
    }

    if (cfg.SqlGoFor.Language == "MSSQL") {
        err = mssql_generate_sql(&cfg, output_sql_file_name)
        if (err == nil) {
            err = mssql_generate_command_parameters(&cfg)
        }
    } else {
        fmt.Printf("Language '%s' is not support", cfg.SqlGoFor.Language)
        return
    }

    if (err == nil) {
        fmt.Println("Done.")
    } else {
        fmt.Println("Error run sqlgo-file:")
        fmt.Println(err)
    }
    return
}

// 2000 --> 8
// 2005 --> 9
// 2008 --> 10
// 2012 --> 11
// 2014 --> 12
// 2016 --> 13
// 2017 --> 14
// 2019 --> 15

func mssql_generate_sql(cfg *Configuration, file_name string) (error) {

    ver, err := strconv.ParseUint(cfg.SqlGoFor.Version, 10, 64)
    if (err != nil) {
        return fmt.Errorf("Version '%s' is not integer! Please enter the version number, for example '2012'.", cfg.SqlGoFor.Version)
    }
    if (ver < 2012) {
        return fmt.Errorf("Version '%s' is not support! Minimal is 2012.", cfg.SqlGoFor.Version)
    }

    file, err := os.Create(file_name)
    if (err != nil) {
        return err
    }
    defer file.Close()

    err = mssql_prepare(file, ver)
    if (err != nil) {
        return err
    }

    for _, table := range cfg.Tables {
        err = mssql_create_table(file, &table, ver)
        if (err != nil) {
            return err
        }

        err = mssql_procedure_list(file, &table, ver)
        if (err != nil) {
            return err
        }

        err = mssql_procedure_merge(file, &table, ver)
        if (err != nil) {
            return err
        }

        err = mssql_procedure_delete(file, &table, ver)
        if (err != nil) {
            return err
        }

        err = mssql_procedure_history(file, &table, ver)
        if (err != nil) {
            return err
        }

        err = mssql_procedure_test(file, &table, ver)
        if (err != nil) {
            return err
        }
    }

    return nil
}

func mssql_prepare(f *os.File, ver uint64) (error) {

    fmt.Fprintln(f, `
IF SCHEMA_ID('Entity') IS NULL
    EXEC('CREATE SCHEMA Entity');
GO

IF SCHEMA_ID('Security') IS NULL
    EXEC('CREATE SCHEMA Security');
GO

IF object_id('[Security].[CheckRights]', 'P') IS NOT NULL
    DROP PROCEDURE [Security].[CheckRights]
GO

CREATE PROCEDURE  [Security].[CheckRights]
    @user_id BIGINT
    ,@project_id BIGINT
    ,@operation NVARCHAR(64)
AS
GO
    `)

    return nil
}

func mssql__drop_procedure_if_exists(f *os.File, procedure_name string, ver uint64) {

    if (ver < 2016) {

        fmt.Fprintf(f, "IF object_id('%s', 'P') IS NOT NULL\n", procedure_name)
        fmt.Fprintf(f, "    DROP PROCEDURE %s\n", procedure_name)
        fmt.Fprintf(f, "GO\n\n")

    } else {
        fmt.Fprintf(f, "DROP PROCEDURE IF EXISTS %s\n", procedure_name)
        fmt.Fprintf(f, "GO\n\n")
    }

}

func mssql_create_table(f *os.File, table *TableDescription, ver uint64) (error) {

    var nullable string

    TableName := fmt.Sprintf("[%s].[%s]", table.Scheme, table.Table)
    one_row := strings.ToLower(table.OneRow)

    if (ver < 2016) {

        fmt.Fprintf(f, "IF object_id('%s', 'U') IS NOT NULL\n", TableName)
        fmt.Fprintf(f, "    DROP TABLE %s\n", TableName)
        fmt.Fprintf(f, "GO\n\n")

    } else {
        fmt.Fprintf(f, "DROP TABLE IF EXISTS %s\n", TableName)
        fmt.Fprintf(f, "GO\n\n")
    }

    fmt.Fprintf(f, "CREATE TABLE %s (\n", TableName)

    fmt.Fprintf(f, "    [%s_id] BIGINT IDENTITY(1,1) NOT NULL\n", one_row)

    if (table.UseHierarhy == true) {
        fmt.Fprintf(f, "    ,[%s_parent_id] BIGINT NOT NULL\n", one_row)
        fmt.Fprintf(f, "    ,[%s_is_folder] BIT NOT NULL\n", one_row)
    }

    if (table.UseProjectId != false) {
        fmt.Fprintf(f, "    ,[project_id] BIGINT NOT NULL\n")
    }

    for _, col := range table.Cols {

        if (col.Nullable == true) {
            nullable = "NULL"
        } else {
            nullable = "NOT NULL"
        }

        fmt.Fprintf(f, "    ,[%s_%s] %s %s\n", one_row, col.Name, strings.ToUpper(col.Type), nullable)
    }

    if (table.UseHistory != true) {
        fmt.Fprintf(f, "    ,[last_hand_user_id] BIGINT NOT NULL\n")
    }
    fmt.Fprintf(f, "    ,[is_delete] BIT NOT NULL\n")
    fmt.Fprintf(f, ")\n")
    fmt.Fprintf(f, "GO\n\n")

    return nil
}

func mssql_procedure_list(f *os.File, table *TableDescription, ver uint64) (error) {

    table_name := fmt.Sprintf("[%s].[%s]", table.Scheme, table.Table)
    procedure_name := fmt.Sprintf("[%s].[%sList]", table.Scheme, table.OneRow)
    one_row := strings.ToLower(table.OneRow)

    mssql__drop_procedure_if_exists(f, procedure_name, ver)

    fmt.Fprintf(f, "CREATE PROCEDURE  %s\n", procedure_name)
    term := ""
    if (table.UseUserId != false) {
        fmt.Fprintf(f, "    @user_id BIGINT\n")
        term = ","
    }
    if (table.UseProjectId != false) {
        fmt.Fprintf(f, "    %s@project_id BIGINT\n", term)
        term = ","
    }
    if (table.UseHierarhy != false) {
        fmt.Fprintf(f, "    %s@%s_parent_id BIGINT\n", term, one_row)
        term = ","
    }
    fmt.Fprintf(f, "AS\n\n")

    if ((table.UseProjectId != false) && (table.UseUserId != false)) {
        fmt.Fprintf(f, "    EXEC [Security].[CheckRights] @user_id, @project_id, 'List';\n")
    }

    fmt.Fprintf(f, "    SET NOCOUNT ON;\n\n")
    fmt.Fprintf(f, "    SELECT\n")
    fmt.Fprintf(f, "        t.[%s_id] [%s_id]\n", one_row, one_row)
    if (table.UseHierarhy != false) {
        fmt.Fprintf(f, "        ,t.[%s_parent_id] [%s_parent_id]\n", one_row, one_row)
        fmt.Fprintf(f, "        ,t.[%s_is_folder] [%s_is_folder]\n", one_row, one_row)
    }

    for _, col := range table.Cols {
        fmt.Fprintf(f, "        ,t.[%s_%s] [%s_%s]\n", one_row, col.Name, one_row, col.Name)
    }

    fmt.Fprintf(f, "    FROM\n")
    fmt.Fprintf(f, "        %s AS t\n", table_name)
    fmt.Fprintf(f, "    WHERE\n")
    fmt.Fprintf(f, "        t.is_delete = 0\n")

    if (table.UseProjectId != false) {
        fmt.Fprintf(f, "        AND t.project_id = project_id\n")
    }
    if (table.UseHierarhy != false) {
        fmt.Fprintf(f, "        AND t.%s_parent_id = %s_parent_id\n", one_row, one_row)
    }

    fmt.Fprintf(f, "    ;\n")
    fmt.Fprintf(f, "RETURN\n")
    fmt.Fprintf(f, "GO\n\n")

    return nil
}

func mssql_procedure_merge(f *os.File, table *TableDescription, ver uint64) (error) {

    // table_name := fmt.Sprintf("[%s].[%s]", table.Scheme, table.Table)
    procedure_name := fmt.Sprintf("[%s].[%sMerge]", table.Scheme, table.OneRow)
    one_row := strings.ToLower(table.OneRow)

    mssql__drop_procedure_if_exists(f, procedure_name, ver)

    fmt.Fprintf(f, "CREATE PROCEDURE  %s\n", procedure_name)
    fmt.Fprintf(f, "    @%s_id BIGINT\n", one_row)

    if (table.UseUserId != false) {
        fmt.Fprintf(f, "    ,@user_id BIGINT\n")
    }
    if (table.UseProjectId != false) {
        fmt.Fprintf(f, "    ,@project_id BIGINT\n")
    }
    if (table.UseHierarhy != false) {
        fmt.Fprintf(f, "    ,@%s_parent_id BIGINT\n", one_row)
    }

    for _, col := range table.Cols {
        fmt.Fprintf(f, "    ,@%s_%s %s\n", one_row, col.Name, strings.ToUpper(col.Type))
    }

    fmt.Fprintf(f, "AS\n\n")

    fmt.Fprintf(f, "GO\n\n")

    return nil
}

func mssql_procedure_delete(f *os.File, table *TableDescription, ver uint64) (error) {
    return nil
}

func mssql_procedure_history(f *os.File, table *TableDescription, ver uint64) (error) {
    return nil
}

func mssql_procedure_test(f *os.File, table *TableDescription, ver uint64) (error) {
    return nil
}

func mssql_generate_command_parameters(cfg *Configuration) (error) {
    return nil
}
