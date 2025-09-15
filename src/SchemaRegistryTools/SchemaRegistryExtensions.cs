using Confluent.SchemaRegistry;
using System;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.SchemaRegistryTools;

public readonly record struct SchemaRegistrationResult(int SchemaId, bool WasCreated);

public static class SchemaRegistryExtensions
{
    public static async Task<SchemaRegistrationResult> RegisterSchemaIfNewAsync(this ISchemaRegistryClient client, string subject, string schema)
    {
        if (client == null) throw new ArgumentNullException(nameof(client));
        if (subject == null) throw new ArgumentNullException(nameof(subject));
        if (schema == null) throw new ArgumentNullException(nameof(schema));

        bool isNew = false;
        try
        {
            var latest = await client.GetLatestSchemaAsync(subject);
            if (latest.SchemaString != schema)
            {
                isNew = true;
            }
            else
            {
                return new SchemaRegistrationResult(latest.Id, false);
            }
        }
        catch (SchemaRegistryException ex) when (ex.ErrorCode == 404 || ex.ErrorCode == 40401)
        {
            isNew = true;
        }

        var sch = new Schema(schema, SchemaType.Avro);
        var id = await client.RegisterSchemaAsync(subject, sch, false);
        return new SchemaRegistrationResult(id, isNew);
    }
}
