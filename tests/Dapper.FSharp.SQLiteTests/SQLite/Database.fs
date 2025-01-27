﻿module Dapper.FSharp.SQLiteTests.SQLite.Database

open Dapper.FSharp.SQLiteTests.Database
open Dapper.FSharp.SQLiteTests.Extensions
open System.Data
open FSharp.Control.Tasks

let init (conn:IDbConnection) =
    task {
        // do! DbName |> sprintf "DROP DATABASE IF EXISTS %s;" |> conn.ExecuteIgnore
        // do! DbName |> sprintf "CREATE DATABASE %s;" |> conn.ExecuteIgnore
        conn.Open()
        //conn.ChangeDatabase DbName
        // do! TestSchema |> sprintf "DROP SCHEMA IF EXISTS %s;" |> conn.ExecuteIgnore
        // do! TestSchema |> sprintf "CREATE SCHEMA %s;" |> conn.ExecuteIgnore
    } |> Async.AwaitTask |> Async.RunSynchronously

module Persons =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE IF EXISTS Persons" |> conn.ExecuteCatchIgnore
            do!
                """
                CREATE TABLE [Persons](
                    [Id] [int] NOT NULL,
                    [FirstName] [text] NOT NULL,
                    [LastName] [text] NOT NULL,
                    [Position] [int] NOT NULL,
                    [DateOfBirth] [datetime] NULL
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Articles =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE Articles" |> conn.ExecuteCatchIgnore
            do!
                """
                create table Articles
                (
                    Id int identity
                        constraint Articles_pk
                            primary key nonclustered,
                    Title [text] not null
                )
                """
                |> conn.ExecuteIgnore
        }

module Dogs =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE Dogs" |> conn.ExecuteCatchIgnore
            do!
                """
                CREATE TABLE [Dogs](
                    [OwnerId] [uniqueidentifier] NOT NULL,
                    [Nickname] [text] NOT NULL
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module VaccinationHistory =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE VaccinationHistory" |> conn.ExecuteCatchIgnore
            do!
                """
                CREATE TABLE [VaccinationHistory] (
                    [PetOwnerId] [uniqueidentifier] NOT NULL,
                    [DogNickname] [text] NOT NULL,
                    [VaccinationDate] [text] NOT NULL
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }
module DogsWeights =

    let init (conn:IDbConnection) =
        task {
            do! "DROP TABLE DogsWeights" |> conn.ExecuteCatchIgnore
            do!
                """
                CREATE TABLE [DogsWeights](
                [DogNickname] [text] NOT NULL,
                [Year] [smallint] NOT NULL,
                [Weight] [smallint] NOT NULL
                )
                """
                |> conn.ExecuteIgnore
            return ()
        }

module Issues =

    module PersonsSimple =

        let init (conn:IDbConnection) =
            task {
                do! "DROP TABLE PersonsSimple" |> conn.ExecuteCatchIgnore
                do!
                    """
                    CREATE TABLE [PersonsSimple](
                    [Id] [int] NOT NULL,
                    [Name] [text] NOT NULL,
                    [Desc] [text] NOT NULL
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }

    module PersonsSimpleDescs =

        let init (conn:IDbConnection) =
            task {
                do! "DROP TABLE PersonsSimpleDescs" |> conn.ExecuteCatchIgnore
                do!
                    """
                    CREATE TABLE [PersonsSimpleDescs](
                    [Id] [int] NOT NULL,
                    [Desc] [text] NOT NULL
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }

    module Group =
        let init (conn:IDbConnection) =
            task {
                do! "DROP TABLE Group" |> conn.ExecuteCatchIgnore
                do!
                    """
                    CREATE TABLE [Group](
                    [Id] [int] NOT NULL,
                    [Name] [text] NOT NULL
                    )
                    """
                    |> conn.ExecuteIgnore
                return ()
            }

    module SchemedGroup =
        let init (conn:IDbConnection) =
            task {
                do! (sprintf "DROP TABLE [%s].SchemedGroup" TestSchema) |> conn.ExecuteCatchIgnore
                do!
                    sprintf """
                    CREATE TABLE [%s].[SchemedGroup](
                    [Id] [int] NOT NULL,
                    [SchemedName] [text] NOT NULL
                    )
                    """ TestSchema
                    |> conn.ExecuteIgnore
                return ()
            }

open Dapper.FSharp.SQLite

let getCrud (conn:IDbConnection) =
    { new ICrudOutput with
        member x.SelectAsync<'a> (q, cancellationToken) = conn.SelectAsync<'a>(q, ?cancellationToken = cancellationToken)
        member x.SelectAsync<'a,'b> q = conn.SelectAsync<'a,'b>(q)
        member x.SelectAsync<'a,'b,'c> q = conn.SelectAsync<'a,'b,'c>(q)
        member x.SelectAsyncOption<'a,'b> q = conn.SelectAsyncOption<'a,'b>(q)
        member x.SelectAsyncOption<'a,'b,'c> q = conn.SelectAsyncOption<'a,'b,'c>(q)
        member x.InsertAsync<'a> (q, cancellationToken) = conn.InsertAsync<'a>(q, ?cancellationToken = cancellationToken)
        member x.DeleteAsync (q, cancellationToken) = conn.DeleteAsync(q, ?cancellationToken = cancellationToken)
        member x.UpdateAsync (q, cancellationToken) = conn.UpdateAsync(q, ?cancellationToken = cancellationToken)
        member x.InsertOutputAsync q = conn.InsertOutputAsync(q)
        member x.DeleteOutputAsync q = conn.DeleteOutputAsync(q)
        member x.UpdateOutputAsync q = conn.UpdateOutputAsync(q)
    }

let getInitializer (conn:IDbConnection) =
    { new ICrudInitializer with
        member x.InitPersons () = Persons.init conn
        member x.InitPersonsSimple () = Issues.PersonsSimple.init conn
        member x.InitPersonsSimpleDescs () = Issues.PersonsSimpleDescs.init conn
        member x.InitArticles () = Articles.init conn
        member x.InitGroups () = Issues.Group.init conn
        member x.InitSchemedGroups () = Issues.SchemedGroup.init conn
        member x.InitDogs () = Dogs.init conn
        member x.InitDogsWeights () = DogsWeights.init conn
        member x.InitVaccinationHistory () = VaccinationHistory.init conn
    }