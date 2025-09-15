using Confluent.SchemaRegistry;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Kafka.Ksql.Linq.Tests;

#nullable enable

internal class FakeSchemaRegistryClient : ISchemaRegistryClient
{
    private readonly Dictionary<string, List<(string Schema, int Id)>> _store = new();
    private int _nextId = 1;

    public IEnumerable<KeyValuePair<string, string>> Config => Array.Empty<KeyValuePair<string, string>>();
    public IAuthenticationHeaderValueProvider? AuthHeaderProvider => null;
    public System.Net.IWebProxy? Proxy => null;
    public int MaxCachedSchemas => 1000;

    public Task<int> RegisterSchemaAsync(string subject, string schema, bool normalize)
    {
        if (!_store.TryGetValue(subject, out var list))
        {
            list = new List<(string, int)>();
            _store[subject] = list;
        }
        var existing = list.Find(p => p.Schema == schema);
        if (existing.Schema != null)
            return Task.FromResult(existing.Id);
        var id = _nextId++;
        list.Add((schema, id));
        return Task.FromResult(id);
    }

    public Task<int> RegisterSchemaAsync(string subject, Schema schema, bool normalize) => RegisterSchemaAsync(subject, schema.SchemaString, normalize);
    public Task<int> RegisterSchemaAsync(string subject, string schema) => RegisterSchemaAsync(subject, schema, false);
    public Task<int> RegisterSchemaAsync(string subject, Schema schema) => RegisterSchemaAsync(subject, schema.SchemaString, false);

    public Task<RegisteredSchema> GetLatestSchemaAsync(string subject)
    {
        if (!_store.TryGetValue(subject, out var list) || list.Count == 0)
            throw new SchemaRegistryException(
                "Subject not found",
                System.Net.HttpStatusCode.NotFound,
                40401);
        var (schema, id) = list[^1];
        var rs = new RegisteredSchema(subject, list.Count, id, schema, SchemaType.Avro, new List<SchemaReference>());
        return Task.FromResult(rs);
    }

    // Unused members
    public Task<RegisteredSchema> RegisterSchemaWithResponseAsync(string subject, Schema schema, bool normalize) => throw new NotImplementedException();
    public Task<int> GetSchemaIdAsync(string subject, string schema, bool normalize) => throw new NotImplementedException();
    public Task<int> GetSchemaIdAsync(string subject, Schema schema, bool normalize) => throw new NotImplementedException();
    public Task<Schema> GetSchemaAsync(int id, string format) => throw new NotImplementedException();
    public Task<Schema> GetSchemaBySubjectAndIdAsync(string subject, int id, string format) => throw new NotImplementedException();
    public Task<Schema> GetSchemaByGuidAsync(string id, string format) => throw new NotImplementedException();
    public Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool normalize, bool lookupDeletedSchema) => throw new NotImplementedException();
    public Task<RegisteredSchema> GetRegisteredSchemaAsync(string subject, int version, bool lookupDeletedSchema) => throw new NotImplementedException();
    public Task<string> GetSchemaAsync(string subject, int version) => throw new NotImplementedException();
    public Task<RegisteredSchema> GetLatestWithMetadataAsync(string subject, IDictionary<string, string> headers, bool ignoreDeletedSchemas) => throw new NotImplementedException();
    public Task<List<string>> GetAllSubjectsAsync()
    {
        var subjects = _store.Keys.ToList();
        return Task.FromResult(subjects);
    }
    public Task<List<int>> GetSubjectVersionsAsync(string subject) => throw new NotImplementedException();
    public Task<bool> IsCompatibleAsync(string subject, string schema) => throw new NotImplementedException();
    public Task<bool> IsCompatibleAsync(string subject, Schema schema) => throw new NotImplementedException();
    public string ConstructKeySubjectName(string subject, string topic) => throw new NotImplementedException();
    public string ConstructValueSubjectName(string subject, string topic) => throw new NotImplementedException();
    public Task<Compatibility> GetCompatibilityAsync(string subject) => throw new NotImplementedException();
    public Task<Compatibility> UpdateCompatibilityAsync(Compatibility compatibility, string subject) => throw new NotImplementedException();
    public void ClearLatestCaches() { }
    public void ClearCaches() { }

    public void Dispose() { }
}
