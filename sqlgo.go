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

type StringSlice []string

type SqlGoForCollect struct {
    Language string
    Version string
}

type DefaultsDescription struct {
    UseHistory bool
    UseHierarhy bool
    UseProjectId bool
    UseUserId bool
    Nullable bool
}

type TableDescription struct {
    Scheme string
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

type Configuration struct {
    SqlGoFor SqlGoForCollect
    Defaults DefaultsDescription
    Tables []TableDescription
}

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

    output_sql_file_name := get_filename_without_ext(file_name) + ".sql"

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
            table_name := fmt.Sprintf("[%s].[%s]", table.Scheme, table.Table)
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
    -- CHECK RIGHTS IS HERE --
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

    table_name := fmt.Sprintf("[%s].[%s]", table.Scheme, table.Table)
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
        fmt.Fprintf(f, "    ,@%s_is_folder BIT\n", one_row)
    }
    for _, col := range table.Cols {
        fmt.Fprintf(f, "    ,@%s_%s %s\n", one_row, col.Name, strings.ToUpper(col.Type))
    }

    fmt.Fprintf(f, "AS\n\n")
    fmt.Fprintf(f, "    DECLARE @processed_record_table TABLE (id BIGINT);\n\n")
    fmt.Fprintf(f, "    SET NOCOUNT ON;\n\n")

    if ((table.UseUserId != false)&&(table.UseProjectId != false)) {
        fmt.Fprintf(f, "    EXEC [Security].[CheckRights] @user_id, @project_id, 'Merge';\n\n")
    }

    fmt.Fprintf(f, "    MERGE %s AS dst\n", table_name)
    fmt.Fprintf(f, "    USING (\n")
    fmt.Fprintf(f, "        SELECT\n")
    fmt.Fprintf(f, "            @%s_id AS [%s_id]\n", one_row, one_row)
    fmt.Fprintf(f, "            ,0 AS [is_delete]\n")

    if (table.UseProjectId != false) {
        fmt.Fprintf(f, "            ,@project_id AS [project_id]\n")
    }
    if (table.UseHierarhy != false) {
        fmt.Fprintf(f, "            ,@%s_parent_id AS [%s_parent_id]\n", one_row, one_row)
        fmt.Fprintf(f, "            ,@%s_is_folder AS [%s_is_folder]\n", one_row, one_row)
    }
    if (table.UseUserId != false)  {
        fmt.Fprintf(f, "            ,@user_id AS [last_hand_user_id]\n") // TODO: это поле всегда должно быть
    }
    for _, col := range table.Cols {
        fmt.Fprintf(f, "            ,@%s_%s AS [%s_%s]\n", one_row, col.Name, one_row, col.Name)
    }

    fmt.Fprintf(f, "    ) AS src\n")
    fmt.Fprintf(f, "    ON (dst.%s_id = src.%s_id)\n", one_row, one_row)
    fmt.Fprintf(f, "    WHEN MATCHED THEN\n")
    fmt.Fprintf(f, "        UPDATE SET\n")
    fmt.Fprintf(f, "            [is_delete] = src.[is_delete]\n")

    if (table.UseProjectId != false) {
        fmt.Fprintf(f, "            ,[project_id] = src.[project_id]\n")
    }
    if (table.UseHierarhy != false) {
        fmt.Fprintf(f, "            ,[%s_parent_id] = src.[%s_parent_id]\n", one_row, one_row)
        fmt.Fprintf(f, "            ,[%s_is_folder] = src.[%s_is_folder]\n", one_row, one_row)
    }
    if (table.UseUserId != false)  {
        fmt.Fprintf(f, "            ,[last_hand_user_id] = src.[last_hand_user_id]\n")
    }
    for _, col := range table.Cols {
        fmt.Fprintf(f, "            ,[%s_%s] = src.[%s_%s]\n", one_row, col.Name, one_row, col.Name)
    }

    fmt.Fprintf(f, "    WHEN NOT MATCHED THEN\n")
    fmt.Fprintf(f, "        INSERT (\n")
    fmt.Fprintf(f, "            [is_delete]\n")

    if (table.UseProjectId != false) {
        fmt.Fprintf(f, "            ,[project_id]\n")
    }
    if (table.UseHierarhy != false) {
        fmt.Fprintf(f, "            ,[%s_parent_id]\n", one_row)
        fmt.Fprintf(f, "            ,[%s_is_folder]\n", one_row)
    }
    if (table.UseUserId != false)  {
        fmt.Fprintf(f, "            ,[last_hand_user_id]\n")
    }
    for _, col := range table.Cols {
        fmt.Fprintf(f, "            ,[%s_%s]\n", one_row, col.Name)
    }

    fmt.Fprintf(f, "        ) VALUES (\n")
    fmt.Fprintf(f, "            src.[is_delete]\n")

    if (table.UseProjectId != false) {
        fmt.Fprintf(f, "            ,src.[project_id]\n")
    }
    if (table.UseHierarhy != false) {
        fmt.Fprintf(f, "            ,src.[%s_parent_id]\n", one_row)
        fmt.Fprintf(f, "            ,src.[%s_is_folder]\n", one_row)
    }
    if (table.UseUserId != false)  {
        fmt.Fprintf(f, "            ,src.[last_hand_user_id]\n")
    }
    for _, col := range table.Cols {
        fmt.Fprintf(f, "            ,src.[%s_%s]\n", one_row, col.Name)
    }
    fmt.Fprintf(f, "    ) OUTPUT\n")
    fmt.Fprintf(f, "        $ACTION AS [action], ISNULL(DELETED.[%s_id], INSERTED.[%s_id]) AS %s_id;\n", one_row, one_row, one_row)
    fmt.Fprintf(f, "RETURN\n")
    fmt.Fprintf(f, "GO\n\n")

    return nil
}

func mssql_procedure_delete(f *os.File, table *TableDescription, ver uint64) (error) {

    table_name := fmt.Sprintf("[%s].[%s]", table.Scheme, table.Table)
    procedure_name := fmt.Sprintf("[%s].[%sDelete]", table.Scheme, table.OneRow)
    one_row := strings.ToLower(table.OneRow)

    mssql__drop_procedure_if_exists(f, procedure_name, ver)

    fmt.Fprintf(f, "CREATE PROCEDURE  %s\n", procedure_name)
    fmt.Fprintf(f, "    @%s_id BIGINT\n", one_row)

    if ((table.UseUserId != false)&&(table.UseProjectId != false)) {
        fmt.Fprintf(f, "    ,@user_id BIGINT\n")
        fmt.Fprintf(f, "    ,@project_id BIGINT\n")
    }

    fmt.Fprintf(f, "AS\n\n")
    fmt.Fprintf(f, "    SET NOCOUNT ON;\n\n")

    if ((table.UseUserId != false)&&(table.UseProjectId != false)) {
        fmt.Fprintf(f, "    EXEC [Security].[CheckRights] @user_id, @project_id, 'Delete';\n\n")
    }

    fmt.Fprintf(f, "    UPDATE %s SET\n", table_name)
    fmt.Fprintf(f, "        is_delete = 1\n")
    fmt.Fprintf(f, "    WHERE\n")
    fmt.Fprintf(f, "        %s_id = @%s_id\n\n", one_row, one_row)
    fmt.Fprintf(f, "RETURN\n")
    fmt.Fprintf(f, "GO\n\n")

    return nil
}

func mssql_procedure_history(f *os.File, table *TableDescription, ver uint64) (error) {
    return nil
}

func mssql_procedure_test(f *os.File, table *TableDescription, ver uint64) (error) {

    // EXEC [Entity].[CategoryMerge] 0, 1, 2, 0, 0, 'test cat new 4', 0, 1.2, 'https://image.url', 1
    return nil
}

func mssql_generate_command_parameters(cfg *Configuration) (error) {
    return nil
}
