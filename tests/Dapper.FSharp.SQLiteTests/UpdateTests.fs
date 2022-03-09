module Dapper.FSharp.SQLiteTests.UpdateTests

open System.Threading.Tasks
open Dapper.FSharp
open Dapper.FSharp.SQLiteTests.Database
open Dapper.FSharp.Builders
open Expecto
open FSharp.Control.Tasks.V2
open System.Threading
open Dapper.FSharp.SQLiteTests.Extensions

type Person = {
    Id: int
    FName: string
    MI: string option
    LName: string
    Age: int
}

let testsDebug (crud:ICrudOutput) (init:ICrudInitializer) = testList "UPDATE DEBUG" [
    let personsView = table'<Persons.View> "Persons"
    testTask "Updates option field to Some and outputs record" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = None })
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            update {
                for p in personsView do
                setColumn p.DateOfBirth (Some (System.DateTime.UtcNow.ToString()))
                where (p.Position = 2L)
            } |> crud.UpdateOutputAsync
        Expect.isSome (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal 2L (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }
]

let testsBasic (crud:ICrud) (init:ICrudInitializer) = testList "UPDATE" [
    
    let personsView = table'<Persons.View> "Persons"

    testTask "Updates single records" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! _ =
            update {
                for p in personsView do
                setColumn p.LastName "UPDATED"
                where (p.Position = 2L)
            } |> crud.UpdateAsync
        let! fromDb =
            select {
                for p in personsView do
                where (p.LastName = "UPDATED")
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 1 (Seq.length fromDb) ""
        Expect.equal 2L (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Cancellation" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync

        use cts = new CancellationTokenSource()
        cts.Cancel()
        let updateCrud query =
            crud.UpdateAsync(query, cancellationToken = cts.Token) :> Task
        let action () = 
            update {
                for p in personsView do
                setColumn p.LastName "UPDATED"
                where (p.Position = 2L)
            } |> updateCrud
        do! Expect.throwsTaskCanceledException action "Should be canceled action"
    }

    testTask "Updates option field to None" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = Some (System.DateTime.UtcNow.ToString()) })
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! _ =
            update {
                for p in personsView do
                setColumn p.DateOfBirth None
                where (p.Position = 2L)
            } |> crud.UpdateAsync
        let! fromDb =
            select {
                for p in personsView do
                where (p.Position = 2L)
            } |> crud.SelectAsync<Persons.View>
        Expect.isNone (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal 2L (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Updates more records" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! _ =
            update {
                for p in personsView do
                setColumn p.LastName "UPDATED"
                where (p.Position > 7L)
            } |> crud.UpdateAsync

        let! fromDb =
            select {
                for p in personsView do
                where (p.LastName = "UPDATED")
            } |> crud.SelectAsync<Persons.View>
        Expect.equal 3 (Seq.length fromDb) ""
    }
    
    testTask "Update with 2 included fields" {
        let person = 
            { Id = 1
              FName = "John"
              MI = None
              LName = "Doe"
              Age = 100 }
    
        let query =
            update {
                for p in table<Person> do
                set person
                includeColumn p.FName
                includeColumn p.LName
            }
            
        Expect.equal query.Fields ["FName"; "LName"] "Expected only 2 fields."
    }
]

let testsOutput (crud:ICrudOutput) (init:ICrudInitializer) = testList "LINQ UPDATE OUTPUT" [
    
    let personsView = table'<Persons.View> "Persons"

    testTask "Updates option field to Some" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = None })
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            update {
                for p in personsView do
                setColumn p.DateOfBirth (Some (System.DateTime.UtcNow.ToString()))
                where (p.Position = 2L)
            } |> crud.UpdateOutputAsync
        Expect.isSome (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal 2L (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }
    
    testTask "Updates and outputs single record" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            update {
                for p in personsView do
                setColumn p.LastName "UPDATED"
                where (p.Position = 2L)
            //} |> crud.UpdateOutputAsync<{| LastName:string |}, Persons.View> // Example how to explicitly declare types
            } |> crud.UpdateOutputAsync
        Expect.equal "UPDATED" (fromDb |> Seq.head |> fun (x:Persons.View) -> x.LastName) ""
        Expect.equal 2L (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Updates and outputs multiple records" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! insertedPersonIds =
            insert {
                into personsView
                values rs
            } |> crud.InsertOutputAsync<Persons.View, {| Id:int64 |}>
        let personIds = insertedPersonIds |> Seq.map (fun (p:{| Id:int64 |}) -> p.Id) |> Seq.toList
        let! updated =
            update {
                for p in personsView do
                setColumn p.LastName "UPDATED"
                where (isIn p.Id personIds)
            } |> crud.UpdateOutputAsync // If we specify the output type after, we dont need to specify it here
        Expect.hasLength updated 10 ""
        updated |> Seq.iter (fun (p:Persons.View) -> // Output specified here
            Expect.equal "UPDATED" (p.LastName) ""
            Expect.isTrue (personIds |> List.exists ((=) p.Id)) "Updated personId not found from inserted Ids")
    }

    testTask "Updates and outputs subset of single record columns" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            update {
                for p in personsView do
                setColumn p.LastName "UPDATED"
                where (p.Position = 2L)
            } |> crud.UpdateOutputAsync
        let pos2Id = rs |> List.pick (fun p -> if p.Position = 2 then Some p.Id else None)
        Expect.equal pos2Id (fromDb |> Seq.head |> fun (p:{| Id:int64 |}) -> p.Id) ""
    }

    testTask "Updates option field to None and outputs record" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = Some (System.DateTime.UtcNow.ToString()) })
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            update {
                for p in personsView do
                setColumn p.DateOfBirth None
                where (p.Position = 2L)
            } |> crud.UpdateOutputAsync
        Expect.isNone (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal 2L (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }

    testTask "Updates option field to Some and outputs record" {
        do! init.InitPersons()
        let rs = Persons.View.generate 10 |> List.map (fun p -> { p with DateOfBirth = None })
        let! _ =
            insert {
                into personsView
                values rs
            } |> crud.InsertAsync
        let! fromDb =
            update {
                for p in personsView do
                setColumn p.DateOfBirth (Some (System.DateTime.UtcNow.ToString()))
                where (p.Position = 2L)
            } |> crud.UpdateOutputAsync
        Expect.isSome (fromDb |> Seq.head |> fun (x:Persons.View) -> x.DateOfBirth) ""
        Expect.equal 2L (fromDb |> Seq.head |> fun (x:Persons.View) -> x.Position) ""
    }
]