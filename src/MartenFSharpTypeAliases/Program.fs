open Newtonsoft.Json

module IdiomaticJson =

  open Microsoft.FSharp.Reflection
  open System

  type IdiomaticDuConverter() =
    inherit JsonConverter()

    [<Literal>]
    let discriminator = "__Case"

    let primitives =
      Set [ JsonToken.Boolean
            JsonToken.Date
            JsonToken.Float
            JsonToken.Integer
            JsonToken.Null
            JsonToken.String ]

    let writeValue (value: obj) (serializer: JsonSerializer, writer: JsonWriter) =
      if value.GetType().IsPrimitive then
        writer.WriteValue value
      else
        serializer.Serialize(writer, value)

    let writeProperties (fields: obj array) (serializer: JsonSerializer, writer: JsonWriter) =
      fields
      |> Array.iteri
           (fun index value ->
             writer.WritePropertyName(sprintf "Item%d" index)
             (serializer, writer) |> writeValue value)

    let writeDiscriminator (name: string) (writer: JsonWriter) =
      writer.WritePropertyName discriminator
      writer.WriteValue name

    override __.WriteJson(writer, value, serializer) =
      let unionCases =
        FSharpType.GetUnionCases(value.GetType())

      let unionType = value.GetType()

      let case, fields =
        FSharpValue.GetUnionFields(value, unionType)

      let allCasesHaveValues =
        unionCases
        |> Seq.forall (fun c -> c.GetFields() |> Seq.length > 0)

      match unionCases.Length, fields, allCasesHaveValues with
      | 2, [||], false -> writer.WriteNull()
      | 1, [| singleValue |], _
      | 2, [| singleValue |], false -> (serializer, writer) |> writeValue singleValue
      | 1, fields, _
      | 2, fields, false ->
          writer.WriteStartObject()
          (serializer, writer) |> writeProperties fields
          writer.WriteEndObject()
      | _ ->
          writer.WriteStartObject()
          writer |> writeDiscriminator case.Name
          (serializer, writer) |> writeProperties fields
          writer.WriteEndObject()

    override __.ReadJson(reader, destinationType, _, _) =
      let parts =
        if reader.TokenType <> JsonToken.StartObject then
          [| (JsonToken.Undefined, obj ()), (reader.TokenType, reader.Value) |]
        else
          seq {
            yield!
              reader
              |> Seq.unfold
                   (fun reader ->
                     if reader.Read() then
                       Some((reader.TokenType, reader.Value), reader)
                     else
                       None)
          }
          |> Seq.takeWhile (fun (token, _) -> token <> JsonToken.EndObject)
          |> Seq.pairwise
          |> Seq.mapi (fun id value -> id, value)
          |> Seq.filter (fun (id, _) -> id % 2 = 0)
          |> Seq.map snd
          |> Seq.toArray

      let values =
        parts
        |> Seq.filter (fun ((_, keyValue), _) -> keyValue <> (discriminator :> obj))
        |> Seq.map snd
        |> Seq.filter (fun (valueToken, _) -> primitives.Contains valueToken)
        |> Seq.map snd
        |> Seq.toArray

      let case =
        let unionCases =
          FSharpType.GetUnionCases(destinationType)

        let unionCase =
          parts
          |> Seq.tryFind (fun ((_, keyValue), _) -> keyValue = (discriminator :> obj))
          |> Option.map (snd >> snd)

        match unionCase with
        | Some case ->
            unionCases
            |> Array.find (fun f -> f.Name :> obj = case)
        | None ->
            // implied union case
            match values with
            | [| null |] ->
                unionCases
                |> Array.find (fun c -> c.GetFields().Length = 0)
            | _ ->
                unionCases
                |> Array.find (fun c -> c.GetFields().Length > 0)

      let values =
        case.GetFields()
        |> Seq.zip values
        |> Seq.map
             (fun (value, propertyInfo) -> Convert.ChangeType(value, propertyInfo.PropertyType))
        |> Seq.toArray

      FSharpValue.MakeUnion(case, values)

    override __.CanConvert(objectType) = FSharpType.IsUnion objectType

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
    let serializer = Marten.Services.JsonNetSerializer()

    serializer.EnumStorage = EnumStorage.AsString
    |> ignore

    serializer.Customize
      (fun jsonSerializer -> jsonSerializer.Converters.Add(IdiomaticJson.IdiomaticDuConverter()))
    // Code directly against a Newtonsoft.Json JsonSerializer


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

        options.Serializer(serializer)
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
