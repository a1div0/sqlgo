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
    "path/filepath"
    "reflect"
)

type Configuration struct {
    SqlGoFor SqlGoForCollect
    Defaults DefaultsDescription
    Tables []TableDescription
}

type SqlGoForCollect struct {
    Language string
    Version string
    Database string
}

type DefaultsDescription struct {
    UseHistory bool
    UseHierarhy bool
    UseProjectId bool
    UseUserId bool
    Nullable bool
}

type TableDescription struct {
    Schema string
    Table string
    OneRow string
    UseHistory interface{}
    UseHierarhy interface{}
    UseProjectId interface{}
    UseUserId interface{}
    Cols []ColumnDescription
}

type ColumnDescription struct {
    Name string
    Type string
    Nullable interface{}
}

type StringSlice []string

func main() {

    var cfg Configuration

    fmt.Println("SQLGO, v1.0")

    if len(os.Args) < 2 {
        fmt.Println("USAGE:")
        fmt.Println("sqlgo.exe <file_name.sqlgo>")
        return
    }

    file_name := os.Args[1]
    file, err := os.Open(file_name)
    if (err != nil) {
        fmt.Printf("Error load '%s': %s\n", file_name, err)
        return
    }

    decoder := json.NewDecoder(file)
    err = decoder.Decode(&cfg)
    if (err != nil) {
        fmt.Printf("Error decode '%s': %s\n", file_name, err)
        return
    }

    err = cfg_fill_and_validate(&cfg)
    if (err != nil) {
        fmt.Printf("Error validate '%s':\n%s\n", file_name, err)
        return
    }

    filename_without_ext := get_filename_without_ext(file_name)
    err = run(&cfg, filename_without_ext)
    if (err == nil) {
        fmt.Println("Done.")
    } else {
        fmt.Println("Error run sqlgo-file:")
        fmt.Println(err)
    }
    return
}

func run(cfg *Configuration, source_filename_without_ext string) (error) {

    var err error

    output_sql_file_name := get_filename_without_ext(source_filename_without_ext) + ".sql"
    output_sqltest_file_name := get_filename_without_ext(source_filename_without_ext) + "_test.sql"

    if (cfg.SqlGoFor.Language == "MSSQL") {
        err = mssql_generate_sql(cfg, output_sql_file_name)
        if (err != nil) {
            return err
        }

        err = mssql_generate_sqltest(cfg, output_sqltest_file_name)
        if (err != nil) {
            return err
        }

        err = mssql_generate_command_parameters(cfg)
        if (err != nil) {
            return err
        }

    } else {
        return fmt.Errorf("Language '%s' is not support", cfg.SqlGoFor.Language)
    }

    return nil
}

func get_filename_without_ext(full_file_name string) (string) {
    name_ext := filepath.Base(full_file_name)
    ext := filepath.Ext(name_ext)
    result := name_ext[:len(name_ext) - len(ext)]
    return result
}

func cfg_fill_and_validate(cfg *Configuration) (error) {

    var errors StringSlice

    for n := 0; n < len(cfg.Tables); n++ {

        table := &cfg.Tables[n]

        field_fill_and_validate(table, "UseHistory"    , cfg.Defaults.UseHistory   , &errors)
        field_fill_and_validate(table, "UseHierarhy"   , cfg.Defaults.UseHierarhy  , &errors)
        field_fill_and_validate(table, "UseProjectId"  , cfg.Defaults.UseProjectId , &errors)
        field_fill_and_validate(table, "UseUserId"     , cfg.Defaults.UseUserId    , &errors)

        for m := 0; m < len(table.Cols); m++ {
            col := &table.Cols[m]
            field_fill_and_validate(col, "Nullable"  , cfg.Defaults.Nullable     , &errors)
        }

        if len(errors) > 0 {
            table_name := fmt.Sprintf("[%s].[%s]", table.Schema, table.Table)
            return fmt.Errorf("Table %s:\n%s\n", table_name, strings.Join(errors, "\n"))
        }

    }

    return nil
}

func field_fill_and_validate(struct_ptr interface{}, field_name string, default_value bool, errors *StringSlice) {

    reflect_struct := reflect.ValueOf(struct_ptr)
    fields := reflect_struct.Elem()

    if (fields.Kind() != reflect.Struct) {
        *errors = append(*errors, "Fields is not struct.")
        return
    }
    field := fields.FieldByName(field_name)
    if !field.IsValid() {
        *errors = append(*errors, fmt.Sprintf("Field '%s' is not valid.", field_name))
        return
    }

    if field.IsNil() {
        if !field.CanSet() {
            *errors = append(*errors, fmt.Sprintf("Field '%s' is not can set.", field_name))
            return
        }
        field.Set(reflect.ValueOf(default_value))
    } else {
        reflect_value := reflect.ValueOf(field.Interface())
        if reflect_value.Kind() != reflect.Bool {
            *errors = append(*errors, fmt.Sprintf("Field %s %s - must be type is bool.", field_name, reflect_value.Type()))
            return
        }
    }

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

    err = mssql_prepare(file, ver, cfg)
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
    }

    err = mssql_finish(file)
    if (err != nil) {
        return err
    }

    return nil
}

func mssql_prepare(f *os.File, ver uint64, cfg *Configuration) (error) {

    script := `
        // USE [master];
        // DECLARE @curr_cdc_enabled BIT = (select is_cdc_enabled from sys.databases db where db.name = '%database%');
        //
        // USE [%database%];
        // IF @curr_cdc_enabled = 1
        // EXECUTE sys.sp_cdc_disable_db
        // GO
        //
        // %script_cdc_on%
        //
        // IF SCHEMA_ID('Entity') IS NULL
        //     EXEC('CREATE SCHEMA Entity');
        // GO
        //
        // IF SCHEMA_ID('Security') IS NULL
        //     EXEC('CREATE SCHEMA Security');
        // GO
        //
        // IF object_id('[Security].[Rights]', 'U') IS NOT NULL
        //     DROP TABLE [Security].[Rights]
        // GO
        //
        // CREATE TABLE [Security].[Rights] (
        //     [right_id] BIGINT IDENTITY(1,1) NOT NULL
        //     ,[user_id] BIGINT NOT NULL
        //     ,[project_id] BIGINT NOT NULL
        // 	,[table_name] NVARCHAR(255) NOT NULL
        //     ,[operation] NVARCHAR(64) NOT NULL
        // )
        // GO
        //
        // IF object_id('[Security].[CheckRights]', 'P') IS NOT NULL
        //     DROP PROCEDURE [Security].[CheckRights]
        // GO
        //
        // CREATE PROCEDURE  [Security].[CheckRights]
        //     @user_id BIGINT
        //     ,@project_id BIGINT
        //     ,@table_name NVARCHAR(255)
        //     ,@operation NVARCHAR(64)
        // AS
        //     -- CHECK RIGHTS IS HERE --
        // GO
    `

    script_cdc_on := `
        // EXECUTE sys.sp_cdc_enable_db;
        // GO
    `

    if global_use_history_get(cfg) {
        script = strings.ReplaceAll(script, "%script_cdc_on%", script_cdc_on)
    }
    script = strings.ReplaceAll(script, "        // ", "")
    script = strings.ReplaceAll(script, "        //", "")
    script = strings.ReplaceAll(script, "%database%", cfg.SqlGoFor.Database)

    fmt.Fprintln(f, script)

    return nil
}

func mssql_finish(f *os.File) (error) {
    script := `
    `
    script = strings.ReplaceAll(script, "        // ", "")
    script = strings.ReplaceAll(script, "        //", "")

    fmt.Fprintln(f, script)

    return nil
}

func mssql__drop_procedure_if_exists(f *os.File, procedure_name string, ver uint64) {

    if ver < 2016 {
        fmt.Fprintf(f, "IF object_id('%s', 'P') IS NOT NULL\n", procedure_name)
        fmt.Fprintf(f, "    DROP PROCEDURE %s;\n", procedure_name)
    } else {
        fmt.Fprintf(f, "DROP PROCEDURE IF EXISTS %s;\n", procedure_name)
    }

    fmt.Fprintln(f, "GO")
    fmt.Fprintln(f, "")
}

func init_names(table *TableDescription, procedure_postfix string) (string, string, string) {

    table_name := fmt.Sprintf("[%s].[%s]", table.Schema, table.Table)
    one_row := strings.ToLower(table.OneRow)
    proc_name := fmt.Sprintf("[%s].[%s%s]", table.Schema, table.OneRow, procedure_postfix)

    return table_name, one_row, proc_name
}

func default_procedure_top(f *os.File, table_name string, use_user_id bool, use_project_id bool, op string) {

    if use_user_id {
        if use_project_id {
            fmt.Fprintf(f, "    EXEC [Security].[CheckRights] @user_id, @project_id, '%s', '%s';\n", table_name, op)
        } else {
            fmt.Fprintf(f, "    EXEC [Security].[CheckRights] @user_id, 0, '%s', '%s';\n", table_name, op)
        }
    }
    fmt.Fprintf(f, "    SET NOCOUNT ON;\n\n")

}

func mssql_create_table(f *os.File, table *TableDescription, ver uint64) (error) {

    var nullable string

    table_name, one_row, _ := init_names(table, "")

    if (ver < 2016) {

        fmt.Fprintf(f, "IF object_id('%s', 'U') IS NOT NULL\n", table_name)
        fmt.Fprintf(f, "    DROP TABLE %s;\n", table_name)

    } else {
        fmt.Fprintf(f, "DROP TABLE IF EXISTS %s;\n", table_name)
    }

    fmt.Fprintln(f, "GO")
    fmt.Fprintln(f, "")

    fmt.Fprintf(f, "CREATE TABLE %s (\n", table_name)

    fmt.Fprintf(f, "    [%s_id] BIGINT IDENTITY(1,1) NOT NULL\n", one_row)

    if table.UseHierarhy.(bool) {
        fmt.Fprintf(f, "    ,[%s_parent_id] BIGINT NOT NULL\n", one_row)
        fmt.Fprintf(f, "    ,[%s_is_folder] BIT NOT NULL\n", one_row)
    }

    if table.UseProjectId.(bool) {
        fmt.Fprintf(f, "    ,[project_id] BIGINT NOT NULL\n")
    }

    for _, col := range table.Cols {

        if col.Nullable.(bool) {
            nullable = "NULL"
        } else {
            nullable = "NOT NULL"
        }

        fmt.Fprintf(f, "    ,[%s_%s] %s %s\n", one_row, col.Name, strings.ToUpper(col.Type), nullable)
    }
    if table.UseUserId.(bool) {
        fmt.Fprintf(f, "    ,[user_id] BIGINT NOT NULL\n")
    }
    fmt.Fprintf(f, "    ,[dt] DATETIME NOT NULL\n")
    fmt.Fprintf(f, "    ,[is_delete] BIT NOT NULL\n")
    fmt.Fprintf(f, "    ,CONSTRAINT PK_%s_%sID PRIMARY KEY CLUSTERED ([%s_id] ASC)\n", table.Table, table.OneRow, one_row)
    fmt.Fprintf(f, ")\n")

    fmt.Fprintln(f, "GO")
    fmt.Fprintln(f, "")

    history_script := ""

    if table.UseHistory.(bool) {
        history_script = `
            // 	EXEC sys.sp_cdc_enable_table
            // 		@source_schema = N'%schema%',
            // 		@source_name   = N'%table%',
            // 		@role_name     = NULL,
            //      @capture_instance = N'%schema%_%table%',
            // 		@supports_net_changes = 1
        `
    }

    history_script = strings.ReplaceAll(history_script, "            // ", "")
    history_script = strings.ReplaceAll(history_script, "            //", "")
    history_script = strings.ReplaceAll(history_script, "%schema%", table.Schema)
    history_script = strings.ReplaceAll(history_script, "%table%", table.Table)

    fmt.Fprintf(f, history_script)

    fmt.Fprintln(f, "GO")
    fmt.Fprintln(f, "")

    return nil
}

func mssql_procedure_list(f *os.File, table *TableDescription, ver uint64) (error) {

    table_name, one_row, procedure_name := init_names(table, "List")
    mssql__drop_procedure_if_exists(f, procedure_name, ver)

    fmt.Fprintf(f, "CREATE PROCEDURE  %s\n", procedure_name)
    term := ""
    if table.UseUserId.(bool) {
        fmt.Fprintf(f, "    @user_id BIGINT\n")
        term = ","
    }
    if table.UseProjectId.(bool) {
        fmt.Fprintf(f, "    %s@project_id BIGINT\n", term)
        term = ","
    }
    if table.UseHierarhy.(bool) {
        fmt.Fprintf(f, "    %s@%s_parent_id BIGINT\n", term, one_row)
        term = ","
    }
    fmt.Fprintf(f, "AS\n\n")
    default_procedure_top(f, table_name, table.UseUserId.(bool), table.UseProjectId.(bool), "List")

    fmt.Fprintf(f, "    SELECT\n")
    fmt.Fprintf(f, "        t.[%s_id] [%s_id]\n", one_row, one_row)
    if table.UseHierarhy.(bool) {
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

    if table.UseProjectId.(bool) {
        fmt.Fprintf(f, "        AND t.project_id = project_id\n")
    }
    if table.UseHierarhy.(bool) {
        fmt.Fprintf(f, "        AND t.%s_parent_id = %s_parent_id\n", one_row, one_row)
    }

    fmt.Fprintf(f, "    ;\n")
    fmt.Fprintf(f, "RETURN\n\n")

    fmt.Fprintln(f, "GO")
    fmt.Fprintln(f, "")

    return nil
}

func mssql_procedure_merge(f *os.File, table *TableDescription, ver uint64) (error) {

    table_name, one_row, procedure_name := init_names(table, "Merge")
    mssql__drop_procedure_if_exists(f, procedure_name, ver)

    fmt.Fprintf(f, "CREATE PROCEDURE  %s\n", procedure_name)
    fmt.Fprintf(f, "    @%s_id BIGINT\n", one_row)

    if table.UseUserId.(bool) {
        fmt.Fprintf(f, "    ,@user_id BIGINT\n")
    }
    if table.UseProjectId.(bool) {
        fmt.Fprintf(f, "    ,@project_id BIGINT\n")
    }
    if table.UseHierarhy.(bool) {
        fmt.Fprintf(f, "    ,@%s_parent_id BIGINT\n", one_row)
        fmt.Fprintf(f, "    ,@%s_is_folder BIT\n", one_row)
    }
    for _, col := range table.Cols {
        fmt.Fprintf(f, "    ,@%s_%s %s\n", one_row, col.Name, strings.ToUpper(col.Type))
    }

    fmt.Fprintf(f, "AS\n\n")
    default_procedure_top(f, table_name, table.UseUserId.(bool), table.UseProjectId.(bool), "Merge")

    fmt.Fprintf(f, "    MERGE %s AS dst\n", table_name)
    fmt.Fprintf(f, "    USING (\n")
    fmt.Fprintf(f, "        SELECT\n")
    fmt.Fprintf(f, "            @%s_id AS [%s_id]\n", one_row, one_row)
    fmt.Fprintf(f, "            ,0 AS [is_delete]\n")
    fmt.Fprintf(f, "            ,GETDATE() AS [dt]\n")

    if table.UseUserId.(bool)  {
        fmt.Fprintf(f, "            ,@user_id AS [user_id]\n")
    }
    if table.UseProjectId.(bool) {
        fmt.Fprintf(f, "            ,@project_id AS [project_id]\n")
    }
    if table.UseHierarhy.(bool) {
        fmt.Fprintf(f, "            ,@%s_parent_id AS [%s_parent_id]\n", one_row, one_row)
        fmt.Fprintf(f, "            ,@%s_is_folder AS [%s_is_folder]\n", one_row, one_row)
    }
    for _, col := range table.Cols {
        fmt.Fprintf(f, "            ,@%s_%s AS [%s_%s]\n", one_row, col.Name, one_row, col.Name)
    }

    fmt.Fprintf(f, "    ) AS src\n")
    fmt.Fprintf(f, "    ON (dst.%s_id = src.%s_id)\n", one_row, one_row)
    fmt.Fprintf(f, "    WHEN MATCHED THEN\n")
    fmt.Fprintf(f, "        UPDATE SET\n")
    fmt.Fprintf(f, "            [is_delete] = src.[is_delete]\n")
    fmt.Fprintf(f, "            ,[dt] = src.[dt]\n")

    if table.UseProjectId.(bool) {
        fmt.Fprintf(f, "            ,[project_id] = src.[project_id]\n")
    }
    if table.UseHierarhy.(bool) {
        fmt.Fprintf(f, "            ,[%s_parent_id] = src.[%s_parent_id]\n", one_row, one_row)
        fmt.Fprintf(f, "            ,[%s_is_folder] = src.[%s_is_folder]\n", one_row, one_row)
    }
    if table.UseUserId.(bool)  {
        fmt.Fprintf(f, "            ,[user_id] = src.[user_id]\n")
    }
    for _, col := range table.Cols {
        fmt.Fprintf(f, "            ,[%s_%s] = src.[%s_%s]\n", one_row, col.Name, one_row, col.Name)
    }

    fmt.Fprintf(f, "    WHEN NOT MATCHED THEN\n")
    fmt.Fprintf(f, "        INSERT (\n")
    fmt.Fprintf(f, "            [is_delete]\n")
    fmt.Fprintf(f, "            ,[dt]\n")

    if table.UseProjectId.(bool) {
        fmt.Fprintf(f, "            ,[project_id]\n")
    }
    if table.UseHierarhy.(bool) {
        fmt.Fprintf(f, "            ,[%s_parent_id]\n", one_row)
        fmt.Fprintf(f, "            ,[%s_is_folder]\n", one_row)
    }
    if table.UseUserId.(bool)  {
        fmt.Fprintf(f, "            ,[user_id]\n")
    }
    for _, col := range table.Cols {
        fmt.Fprintf(f, "            ,[%s_%s]\n", one_row, col.Name)
    }

    fmt.Fprintf(f, "        ) VALUES (\n")
    fmt.Fprintf(f, "            src.[is_delete]\n")
    fmt.Fprintf(f, "            ,src.[dt]\n")

    if table.UseProjectId.(bool) {
        fmt.Fprintf(f, "            ,src.[project_id]\n")
    }
    if table.UseHierarhy.(bool) {
        fmt.Fprintf(f, "            ,src.[%s_parent_id]\n", one_row)
        fmt.Fprintf(f, "            ,src.[%s_is_folder]\n", one_row)
    }
    if table.UseUserId.(bool) {
        fmt.Fprintf(f, "            ,src.[user_id]\n")
    }
    for _, col := range table.Cols {
        fmt.Fprintf(f, "            ,src.[%s_%s]\n", one_row, col.Name)
    }
    fmt.Fprintf(f, "    ) OUTPUT\n")
    fmt.Fprintf(f, "        $ACTION AS [action], ISNULL(DELETED.[%s_id], INSERTED.[%s_id]) AS %s_id;\n", one_row, one_row, one_row)
    fmt.Fprintf(f, "RETURN\n\n")

    fmt.Fprintln(f, "GO")
    fmt.Fprintln(f, "")

    return nil
}

func mssql_procedure_delete(f *os.File, table *TableDescription, ver uint64) (error) {

    table_name, one_row, procedure_name := init_names(table, "Delete")
    mssql__drop_procedure_if_exists(f, procedure_name, ver)

    fmt.Fprintf(f, "CREATE PROCEDURE  %s\n", procedure_name)
    fmt.Fprintf(f, "    @%s_id BIGINT\n", one_row)

    if table.UseUserId.(bool) {
        fmt.Fprintf(f, "    ,@user_id BIGINT\n")
    }
    if table.UseProjectId.(bool) {
        fmt.Fprintf(f, "    ,@project_id BIGINT\n")
    }

    fmt.Fprintf(f, "AS\n\n")
    default_procedure_top(f, table_name, table.UseUserId.(bool), table.UseProjectId.(bool), "Delete")

    script := `
        //     UPDATE %table_name% SET
        //         is_delete = 1
        //         ,dt = GETDATE()
        //         {,user_id = @user_id}
        //     WHERE
        //         %one_row%_id = @%one_row%_id
    `

    script = strings.ReplaceAll(script, "        // ", "")
    script = strings.ReplaceAll(script, "%table_name%", table_name)
    script = strings.ReplaceAll(script, "%one_row%", one_row)

    if table.UseUserId.(bool) {
        script = strings.ReplaceAll(script, "{,user_id = @user_id}", ",user_id = @user_id")
    } else {
        script = strings.ReplaceAll(script, "{,user_id = @user_id}", "")
    }

    fmt.Fprintf(f, script)
    fmt.Fprintf(f, "RETURN\n\n")

    fmt.Fprintln(f, "GO")
    fmt.Fprintln(f, "")

    return nil
}

func mssql_procedure_history(f *os.File, table *TableDescription, ver uint64) (error) {
    if !table.UseHistory.(bool) {
        return nil
    }

    table_name, one_row, procedure_name := init_names(table, "History")
    mssql__drop_procedure_if_exists(f, procedure_name, ver)

    fmt.Fprintf(f, "CREATE PROCEDURE  %s\n", procedure_name)
    fmt.Fprintf(f, "    @%s_id BIGINT\n", one_row)

    if table.UseUserId.(bool) {
        fmt.Fprintf(f, "    ,@user_id BIGINT\n")
    }
    if table.UseProjectId.(bool) {
        fmt.Fprintf(f, "    ,@project_id BIGINT\n")
    }

    fmt.Fprintf(f, "AS\n\n")
    default_procedure_top(f, table_name, table.UseUserId.(bool), table.UseProjectId.(bool), "History")

    script := `
        //     SELECT
        //         *
        //     FROM
        //         cdc.%schema%_%table%_CT
        //     WHERE
        //         [%one_row%_id] = @%one_row%_id
        //     ORDER BY
        //         __$start_lsn DESC
    `

    script = strings.ReplaceAll(script, "        // ", "")
    script = strings.ReplaceAll(script, "%schema%", table.Schema)
    script = strings.ReplaceAll(script, "%table%", table.Table)
    script = strings.ReplaceAll(script, "%one_row%", one_row)

    fmt.Fprintf(f, script)
    fmt.Fprintf(f, "RETURN\n")

    fmt.Fprintln(f, "GO")
    fmt.Fprintln(f, "")

    return nil
}

func mssql_generate_sqltest(cfg *Configuration, file_name string) (error) {
    file, err := os.Create(file_name)
    if (err != nil) {
        return err
    }
    defer file.Close()

    mssql_test_prepare(file, cfg)

    for _, table := range cfg.Tables {
        err = mssql_test_table(file, &table)
        if (err != nil) {
            return err
        }
    }

    mssql_test_tail(file, cfg)

    return nil
}

func mssql_test_prepare(f *os.File, cfg *Configuration) {
    script := `
        // BEGIN TRAN;
        //
        // SET NOCOUNT ON;
        //
        // CREATE TABLE [Security].[__TestProc__CheckRights] (
        //     [user_id] BIGINT
        //     ,[project_id] BIGINT
        //     ,[table_name] NVARCHAR(255)
        //     ,[operation] NVARCHAR(64)
        // )
        // GO
        //
        // ALTER PROCEDURE [Security].[CheckRights]
        //     @user_id BIGINT
        //     ,@project_id BIGINT
        //     ,@table_name NVARCHAR(255)
        //     ,@operation NVARCHAR(64)
        // AS
        //     INSERT INTO [Security].[__TestProc__CheckRights] ([user_id], [project_id], [table_name], [operation])
        //     VALUES (@user_id, @project_id, @table_name, @operation)
        //
        // RETURN
        // GO
        //
    `

    script = strings.ReplaceAll(script, "        // ", "")
    script = strings.ReplaceAll(script, "        //", "")

    fmt.Fprintln(f, script)
}

type ColumnTestDescription struct {
    Name string
    Type string
    TestValue string
    UseInListResult bool
    //Nullable bool
}

func mssql_test_table(f *os.File, table *TableDescription) (error) {

    script := `
        // DECLARE @%one_row%_merge_result TABLE ([op] NVARCHAR(64), [%one_row%_id] BIGINT);
        // INSERT INTO @%one_row%_merge_result
        // EXEC %MEGRE%;
        //
        // DECLARE @%one_row%_id BIGINT = (SELECT TOP 1 [%one_row%_id] FROM @%one_row%_merge_result);
        // DECLARE @%one_row%_tmp TABLE (%cols_tmp_table_with_types%);
        // INSERT INTO @%one_row%_tmp
        // EXEC %LIST%;
        //
        // INSERT INTO @%one_row%_tmp(%cols_tmp_table%)
        // VALUES (%inserted_values%);
        //
        // DECLARE @%one_row%_res_cnt INT = (SELECT
        //     COUNT(*)
        // FROM
        //     (SELECT DISTINCT * FROM @%one_row%_tmp WHERE [%one_row%_id] = @%one_row%_id) AS t);
        // IF @%one_row%_res_cnt > 1
        // BEGIN
        //     THROW 51000, 'FAIL: List or Insert', 1;
        // END;
        //
        // DELETE FROM @%one_row%_tmp;
        //
        // EXEC %DELETE%;
        //
        // INSERT INTO @%one_row%_tmp
        // EXEC %LIST%;
        //
        // SET @%one_row%_res_cnt = (SELECT
        //     COUNT(*)
        // FROM
        //     (SELECT DISTINCT * FROM @%one_row%_tmp WHERE [%one_row%_id] = @%one_row%_id) AS t);
        // IF @%one_row%_res_cnt > 0
        // BEGIN
        //     THROW 51000, 'FAIL: List or Delete', 1;
        // END;
        //
    `
    script = strings.ReplaceAll(script, "        // ", "")
    script = strings.ReplaceAll(script, "        //", "")

    _, one_row, procedure_name := init_names(table, "Merge")

    cols := make([]ColumnTestDescription, 0, 5 + len(table.Cols))
    cols = append(cols, ColumnTestDescription {Name: one_row + "_id", Type: "bigint", TestValue: "0", UseInListResult: true})

    if table.UseUserId.(bool) {
        cols = append(cols, ColumnTestDescription {Name: "user_id", Type: "bigint", TestValue: "123", UseInListResult: false})
    }
    if table.UseProjectId.(bool) {
        cols = append(cols, ColumnTestDescription {Name: "project_id", Type: "bigint", TestValue: "321", UseInListResult: false})
    }
    if table.UseHierarhy.(bool) {
        cols = append(cols, ColumnTestDescription {Name: one_row + "_parent_id", Type: "bigint", TestValue: "0", UseInListResult: true})
        cols = append(cols, ColumnTestDescription {Name: one_row + "_is_folder", Type: "bit", TestValue: "0", UseInListResult: true})
    }
    for num, col := range table.Cols {
        type_name := strings.ToLower(col.Type)
        value := ""

        if strings.Contains(type_name, "varchar") {
            value = "'" + col.Name + " - dummy value" + "'"
        } else if strings.HasPrefix(type_name, "date") {
            value = "'2020-05-02 15:27:00.000'"
        } else if type_name == "bit" {
            value = "1"
        } else {
            value = fmt.Sprintf("%d", num)
        }

        cols = append(cols, ColumnTestDescription {Name: one_row + "_" + col.Name, Type: col.Type, TestValue: value, UseInListResult: true})
    }

    exec_merge := procedure_name + " " + get_test_values(&cols, false)
    script = strings.ReplaceAll(script, "%MEGRE%", exec_merge)

    cols_tmp_table_with_types := get_test_cols_tmp_table_with_types(&cols)
    script = strings.ReplaceAll(script, "%cols_tmp_table_with_types%", cols_tmp_table_with_types)

    cols_tmp_table:= get_test_cols_tmp_table(&cols)
    script = strings.ReplaceAll(script, "%cols_tmp_table%", cols_tmp_table)

    exec_list := ""
    if table.UseUserId.(bool) {
        exec_list += ",123"
    }
    if table.UseProjectId.(bool) {
        exec_list += ",321"
    }
    if table.UseHierarhy.(bool) {
        exec_list += ",0"
    }
    if (len(exec_list) > 0) {
        exec_list = exec_list[1:]
    }
    _, _, procedure_name = init_names(table, "List")
    script = strings.ReplaceAll(script, "%LIST%", procedure_name + " " + exec_list)

    cols[0].TestValue = "@%one_row%_id"

    inserted_values := get_test_values(&cols, true)
    script = strings.ReplaceAll(script, "%inserted_values%", inserted_values)

    _, _, procedure_name = init_names(table, "Delete")
    exec_list = " @%one_row%_id"
    if table.UseUserId.(bool) {
        exec_list += ",123"
    }
    if table.UseProjectId.(bool) {
        exec_list += ",321"
    }
    script = strings.ReplaceAll(script, "%DELETE%", procedure_name + exec_list)

    script = strings.ReplaceAll(script, "%one_row%", one_row)

    fmt.Fprintln(f, script)

    return nil
}

func mssql_test_tail(f *os.File, cfg *Configuration) {
    script := `
        // SELECT * FROM [Security].[__TestProc__CheckRights]
        //
        // ROLLBACK TRAN
    `

    script = strings.ReplaceAll(script, "        // ", "")
    script = strings.ReplaceAll(script, "        //", "")

    fmt.Fprintln(f, script)
}

func get_test_values(cols *[]ColumnTestDescription, only_list bool) string {
    result := ""
    for _, d := range *cols {
        if (only_list && !d.UseInListResult) {
            continue
        }
    	result += ", " + d.TestValue
    }
    return result[2:]
}

func get_test_cols_tmp_table_with_types(cols *[]ColumnTestDescription) string {
    result := ""
    for _, d := range *cols {
        if d.UseInListResult {
        	result += ", [" + d.Name + "] " + strings.ToUpper(d.Type)
        }
    }
    return result[2:]
}

func get_test_cols_tmp_table(cols *[]ColumnTestDescription) string {
    result := ""
    for _, d := range *cols {
        if d.UseInListResult {
            result += ", [" + d.Name + "]"
        }
    }
    return result[2:]
}

func map_values_to_slice(values *[]string, myMap *map[string]string) {
    *values = make([]string, 0, len(*myMap))

    for _, v := range *myMap {
    	*values = append(*values, v)
    }
}

func global_use_history_get(cfg *Configuration) bool {

    global_use_history := false
    for _, table := range cfg.Tables {
        if !global_use_history && table.UseHistory.(bool) {
            global_use_history = true
            break
        }
    }
    return global_use_history

}

func mssql_generate_command_parameters(cfg *Configuration) (error) {
    return nil
}
