module Domain =
  open System
  type CustomerNumber = CustomerNumber of string

  type RegisteredCustomer =
    { CompanyName: string
      Number: CustomerNumber }

  type DeletedCustomer =
    { CompanyName: string
      DeletedOn: DateTimeOffset }

  type Customer =
    | Unregistered
    | Registered of RegisteredCustomer
    | Deleted of DeletedCustomer

module Commands =
  open Domain

  type CustomerRegistration =
    { CompanyName: string
      Number: CustomerNumber }

  type Command =
    | Register of CustomerRegistration
    | Delete

module DomainEvents =
  open System
  open Domain

  type CustomerRegisteredEvent =
    { CompanyName: string
      Number: CustomerNumber }

  type CustomerDeletedEvent = { DeletedOn: DateTimeOffset }

  type CustomerEvent =
    | CustomerRegistered of CustomerRegisteredEvent
    | CustomerDeleted of CustomerDeletedEvent


module CommandHandler =
  open System
  open Domain
  open Commands
  open DomainEvents

  let decide state command =
    match (command, state) with
    | Register customerRegistration, Unregistered ->
        Ok [ CustomerRegistered
               { CompanyName = customerRegistration.CompanyName
                 Number = customerRegistration.Number } ]
    | Register _, Registered _ -> Error ""
    | Register _, Deleted _ -> Error "Cannot register a deleted customer"
    | Delete, Registered _ -> Ok [ CustomerDeleted { DeletedOn = DateTimeOffset.Now } ]
    | Delete, Unregistered -> Error "Cannot delete a unregistered customer"
    | Delete, Deleted _ -> Error "Not sure we deleted it really? Trust me, we did"

module EventHandler =
  open Domain
  open DomainEvents

  let evolve (state: Customer) event : Customer =
    match event with
    | CustomerRegistered registeredCustomer ->
        Registered
          { CompanyName = registeredCustomer.CompanyName
            Number = registeredCustomer.Number }
    | CustomerDeleted deleted ->
        match state with
        | Registered registered ->
            Deleted
              { DeletedOn = deleted.DeletedOn
                CompanyName = registered.CompanyName }
        | _ -> failwith "todo"

  let build = List.fold evolve
  let rebuild = build Unregistered


module EventStore =
  open System
  open System.Collections.Generic
  open Marten
  open DomainEvents

  type WrappedEvent<'T>(event: 'T) =
    let mutable headers = Dictionary<string, obj>()

    interface Events.IEvent<'T> with
      member self.Data = event

    interface Events.IEvent with
      member self.Data = event :> obj
      member val DotNetTypeName = null with get, set
      member val EventType = null
      member val EventTypeName = null with get, set
      member val Id = Guid.Empty with get, set
      member val Sequence = int64 (0) with get, set
      member val StreamId = Guid.Empty with get, set
      member val StreamKey = null with get, set
      member val TenantId = null with get, set
      member val Timestamp = DateTimeOffset.MinValue with get, set
      member val Version = int64 (0) with get, set
      member val CorrelationId = String.Empty with get, set
      member val CausationId = String.Empty with get, set

      member this.Headers
        with get () = headers
        and set (v) = headers <- v

      member this.SetHeader(key, value) = headers.Add(key, value)
      member this.GetHeader(key) = headers.[key]


  let MapToSubtype subtype =
    match subtype with
    | CustomerRegistered registeredCustomer ->
        WrappedEvent<CustomerRegisteredEvent> registeredCustomer :> Events.IEvent
    | CustomerDeleted deletedCustomer ->
        WrappedEvent<CustomerDeletedEvent> deletedCustomer :> Events.IEvent


  let store =
    DocumentStore.For
      (fun options ->
        let connectionString =
          sprintf
            "host=%s;port=%i;database=%s;username=%s;password=%s"
            "localhost"
            5432
            "test_events"
            "marten"
            "123456"

        options.Connection(connectionString)
        options.AutoCreateSchemaObjects <- AutoCreate.All)

  let save (streamId: Guid) events =
    use session = store.OpenSession()

    let eventData =
      events
      |> List.map (fun e -> MapToSubtype e)
      |> List.map (fun (e: Events.IEvent) -> e.Data)
      |> List.toArray

    session.Events.Append(streamId, eventData)
    |> ignore

    session.SaveChangesAsync()
    |> Async.AwaitTask
    |> Async.RunSynchronously

  open FSharp.Reflection

  let getCaseMap<'t> () =
    FSharpType.GetUnionCases(typeof<'t>)
    |> Seq.map
         (fun unionCase ->
           let typ =
             let property = unionCase.GetFields() |> Seq.exactlyOne
             property.PropertyType

           let create data =
             FSharpValue.MakeUnion(unionCase, [| data |]) :?> 't

           typ.Name, create)
    |> Map


  let load (streamId: Guid) =
    use session = store.OpenSession()
    let stream = session.Events.FetchStream(streamId)
    let caseMap = getCaseMap<CustomerEvent> ()

    // map IEvent to IEvent<T> where T = CustomerEvent
    let map (input: Events.IEvent) =
      let typ =
        input.GetType().GenericTypeArguments
        |> Seq.exactlyOne

      let data =
        let property = input.GetType().GetProperty("Data")
        property.GetValue(input)

      caseMap.[typ.Name] data

    let events = stream |> Seq.map map |> Seq.toList

    events

open System
open Domain
open Commands
open CommandHandler
open EventHandler

[<EntryPoint>]
let main _ =
  let register =
    Register
      { CompanyName = "Some Company"
        Number = CustomerNumber "0001" }

  let registeredCustomer = decide Unregistered register
  let id = Guid.NewGuid()

  match registeredCustomer with
  | Ok customer ->
      EventStore.save id customer
      let events = EventStore.load id
      let customerState : Customer = rebuild events

      match customerState with
      | Registered customer ->
          printfn
            $"successfully registered customer {customer.CompanyName} with Number {customer.Number}"
      | _ -> failwith "todo"

  | Error error -> printfn $"Error handling registration: {error}"

  0 // return an integer exit code
