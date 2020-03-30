// 1div0
// 2020.03.30

package main

// Раздел импорта
import (
    "fmt" // пакет для форматированного ввода вывода
    "os"
    //"strings"
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
    History bool
    Hierarhy bool
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

func mssql_generate_sql(cfg *Configuration, file_name string) (error) {

    file, err := os.Create(file_name)
    if (err != nil) {
        return err
    }
    defer file.Close()

    for _, table := range cfg.Tables {
        err = mssql_create_table(file, &table)
        if (err != nil) {
            return err
        }

        err = mssql_storage_list(file, &table)
        if (err != nil) {
            return err
        }

        err = mssql_storage_merge(file, &table)
        if (err != nil) {
            return err
        }

        err = mssql_storage_delete(file, &table)
        if (err != nil) {
            return err
        }

        err = mssql_storage_history(file, &table)
        if (err != nil) {
            return err
        }
    }

    return nil
}

func mssql_create_table(f *os.File, table *TableDescription) (error) {

    f.WriteString(table.Table)
    return nil
}

func mssql_storage_list(f *os.File, table *TableDescription) (error) {
    return nil
}

func mssql_storage_merge(f *os.File, table *TableDescription) (error) {
    return nil
}

func mssql_storage_delete(f *os.File, table *TableDescription) (error) {
    return nil
}

func mssql_storage_history(f *os.File, table *TableDescription) (error) {
    return nil
}

func mssql_generate_command_parameters(cfg *Configuration) (error) {
    return nil
}
