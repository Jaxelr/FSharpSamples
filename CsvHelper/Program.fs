open FSharp.Data
open Insight.Database
open System.Data.SqlClient

let data = new CsvProvider<"uber.csv", HasHeaders=false, IgnoreErrors=true, Schema="MedicaidId (string), Npi (string)">()

[<Literal>]
let ConnectionString = "Server=.;database=master;Trusted_Connection=true;MultipleActiveResultSets=true;"

[<EntryPoint>]
let main argv =
    
    let connection = new SqlConnection(ConnectionString)
    SqlInsightDbProvider.RegisterProvider()

    let deleted = connection.ExecuteSql("DELETE FROM internal.[key]")

    for row in data.Rows do
        if (row.MedicaidId <> "999999999" && row.Npi <> "") then
            
            let parameters = 
                {| 
                    Npi = row.Npi 
                    MedicaidId = row.MedicaidId 
                |}

            let response = connection.ExecuteSql("IF NOT EXISTS(SELECT 1 FROM internal.[key] WHERE Npi = @Npi) 
                                                    BEGIN
	                                                INSERT INTO internal.[Key](MedicaidId, Npi) SELECT @MedicaidId, @Npi
                                                    END", 
                                                parameters)
            printfn "Inserted %i record" response
    0 // return an integer exit code