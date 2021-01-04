open FSharp.Data
open Insight.Database
open System.Data.SqlClient

let data = new CsvProvider<"C:\\temp\\uber.csv", HasHeaders=false, IgnoreErrors=true, Schema="MedicaidId (string), Npi (string),,,FullName (string),,,,,,,,,,,,,State (string)">()

[<Literal>]
let ConnectionString = "Server=(local);database=master;Trusted_Connection=true;MultipleActiveResultSets=true;"

[<EntryPoint>]
let main argv =
    
    use connection = new SqlConnection(ConnectionString)
    SqlInsightDbProvider.RegisterProvider()

    connection.ExecuteSql("DELETE FROM internal.[key]") |> ignore

    for row in data.Rows do
        if (row.MedicaidId <> "999999999" && row.Npi <> "") then
            
            let parameters = 
                {| 
                    Npi = row.Npi 
                    MedicaidId = row.MedicaidId 
                    State = row.State
                    FullName = row.FullName
                |}

            let response = connection.ExecuteSql("IF NOT EXISTS(SELECT 1 FROM internal.[key] WHERE Npi = @Npi) 
                                                    BEGIN
	                                                INSERT INTO internal.[Key](MedicaidId, Npi, State, FullName) SELECT @MedicaidId, @Npi, @State, @FullName
                                                    END", 
                                                parameters)
            printfn "Inserted %i record" response
    0 // return an integer exit code