module Dapper.FSharp.SQLiteTests.Program

open Dapper.FSharp.SQLiteTests.Database
open Expecto
open Expecto.Logging
open Microsoft.Data.SqlClient
open Microsoft.Extensions.Configuration
open MySql.Data.MySqlClient
open Npgsql
open Microsoft.Data.Sqlite

let testConfig =
    { defaultConfig with
        parallelWorkers = 4
        verbosity = LogLevel.Debug }


let private debugTests crud init = [
    //SelectTests.testsDebug crud init
    //InsertTests.testsDebug crud init
    UpdateTests.testsDebug crud init
]

let private sharedTests crud init = [
    InsertTests.testsBasic crud init
    DeleteTests.testsBasic crud init
    UpdateTests.testsBasic crud init
    SelectTests.testsBasic crud init
    SelectQueryBuilderTests.tests
    //TODO
    //IssuesTests.testsBasic crud init
    //IssuesTests.testsOutput crud init
]

let private sharedTestsWithOutputSupport crud init = [
    DeleteTests.testsOutput crud init
    UpdateTests.testsOutput crud init
    InsertTests.testsOutput crud init
]

let sqliteTests connString =
    let conn = new SqliteConnection(connString)
    conn |> SQLite.Database.init
    let crud = SQLite.Database.getCrud conn
    let init = SQLite.Database.getInitializer conn
    sharedTests crud init
    @ sharedTestsWithOutputSupport crud init
    @ [
        SQLite.AggregatesTests.tests conn
    ]
    |> testList "SQLite"
    |> testSequenced

let sqliteDebugTests connString =
    let conn = new SqliteConnection(connString)
    conn |> SQLite.Database.init
    let crud = SQLite.Database.getCrud conn
    let init = SQLite.Database.getInitializer conn
    debugTests crud init
    @ [ SQLite.AggregatesTests.debugTests conn ]
    |> testList "SQLite"
    |> testSequenced

[<EntryPoint>]
let main argv =

    let conf = (ConfigurationBuilder()).AddJsonFile("settings.json").Build()
    Dapper.FSharp.OptionTypes.register()
    [
        conf.["sqliteConnectionString"] |> sqliteTests
        //conf.["sqliteConnectionString"] |> sqliteDebugTests
    ]
    |> testList "✔"
    |> runTests testConfig
