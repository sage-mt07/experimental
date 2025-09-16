using Kafka.Ksql.Linq.Core.Modeling;
using Kafka.Ksql.Linq.Query.Abstractions;
using Kafka.Ksql.Linq.Query.Builders;
using Kafka.Ksql.Linq.Query.Builders.Common;
using Kafka.Ksql.Linq.Query.Ddl;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Pipeline;

/// <summary>
/// DDL query generator (new builder version)
/// Rationale: separation-of-concerns; generate CREATE STREAM/TABLE via integrated builders
/// </summary>
internal class DDLQueryGenerator : GeneratorBase, IDDLQueryGenerator
{
    /// <summary>
    /// Constructor (builder dependency injection)
    /// </summary>
    public DDLQueryGenerator(IReadOnlyDictionary<KsqlBuilderType, IKsqlBuilder> builders)
        : base(builders)
    {
    }

    /// <summary>
    /// Simplified constructor (uses standard builders)
    /// </summary>
    public DDLQueryGenerator() : this(CreateStandardBuilders())
    {
    }

    protected override KsqlBuilderType[] GetRequiredBuilderTypes()
    {
        return new[]
        {
            KsqlBuilderType.Select,
            KsqlBuilderType.Where,
            KsqlBuilderType.GroupBy,
        };
    }

    private static string SanitizeName(string name) => name.Replace("-", "_");

    /// <summary>
    /// Generate CREATE STREAM statement
    /// </summary>
    public string GenerateCreateStream(IDdlSchemaProvider provider)
    {
        ModelCreatingScope.EnsureInScope();
        DdlSchemaDefinition? schema = null;
        try
        {
            schema = provider.GetSchema();
            var columns = GenerateColumnDefinitions(schema, isStream: true);
            var streamName = SanitizeName(schema.TopicName);
            var topicName = schema.TopicName;
            var hasKey = schema.Columns.Any(c => c.IsKey);
            var partitions = schema.Partitions;
            var replicas = schema.Replicas;

            var withParts = new List<string> { $"KAFKA_TOPIC='{topicName}'" };
            // Streams without key columns must not specify KEY_FORMAT
            if (hasKey)
            {
                withParts.Add("KEY_FORMAT='AVRO'");
            }
            withParts.Add("VALUE_FORMAT='AVRO'");
            if (!string.IsNullOrWhiteSpace(schema.ValueSchemaFullName))
                withParts.Add($"VALUE_AVRO_SCHEMA_FULL_NAME='{schema.ValueSchemaFullName}'");
            if (!string.IsNullOrWhiteSpace(schema.TimestampColumn))
                withParts.Add($"TIMESTAMP='{schema.TimestampColumn}'");
            withParts.Add($"PARTITIONS={partitions}");
            withParts.Add($"REPLICAS={replicas}");
            var withClause = string.Join(", ", withParts);

            var query = $"CREATE OR REPLACE STREAM {streamName} ({columns}) WITH ({withClause});";

            return query;
        }
        catch (Exception ex)
        {
            return HandleGenerationError("CREATE STREAM generation", ex, $"Stream: {schema?.ObjectName}, Topic: {schema?.TopicName}");
        }
    }

    /// <summary>
    /// Generate CREATE TABLE statement
    /// </summary>
    public string GenerateCreateTable(IDdlSchemaProvider provider)
    {
        ModelCreatingScope.EnsureInScope();
        DdlSchemaDefinition? schema = null;
        try
        {
            schema = provider.GetSchema();
            var columns = GenerateColumnDefinitions(schema, isStream: false);
            var tableName = SanitizeName(schema.TopicName);
            var topicName = schema.TopicName;
            var hasKey = schema.Columns.Any(c => c.IsKey);
            var partitions = schema.Partitions;
            var replicas = schema.Replicas;

            var withParts = new List<string> { $"KAFKA_TOPIC='{topicName}'" };
            // TABLE は通常主キーを持つが、念のためキー定義がある場合のみ KEY_FORMAT を付与
            if (hasKey)
            {
                withParts.Add("KEY_FORMAT='AVRO'");
            }
            withParts.Add("VALUE_FORMAT='AVRO'");
            if (!string.IsNullOrWhiteSpace(schema.ValueSchemaFullName))
                withParts.Add($"VALUE_AVRO_SCHEMA_FULL_NAME='{schema.ValueSchemaFullName}'");
            withParts.Add($"PARTITIONS={partitions}");
            withParts.Add($"REPLICAS={replicas}");
            var withClause = string.Join(", ", withParts);

            var query = $"CREATE TABLE IF NOT EXISTS {tableName} ({columns}) WITH ({withClause});";

            return query;
        }
        catch (Exception ex)
        {
            return HandleGenerationError("CREATE TABLE generation", ex, $"Table: {schema?.ObjectName}, Topic: {schema?.TopicName}");
        }
    }
    /// <summary>
    /// Generate CREATE STREAM AS statement
    /// </summary>
    public string GenerateCreateStreamAs(string streamName, string baseObject, Expression linqExpression)
    {
        ModelCreatingScope.EnsureInScope();
        try
        {
            var context = new QueryAssemblyContext(baseObject, false); // Push Query
            var structure = CreateStreamAsStructure(streamName, baseObject);

            // Analyze LINQ expression to build query clauses
            structure = ProcessLinqExpression(structure, linqExpression, context);

            var query = AssembleStructuredQuery(structure);
            return ApplyQueryPostProcessing(query, context);
        }
        catch (Exception ex)
        {
            return HandleGenerationError("CREATE STREAM AS generation", ex, $"Stream: {streamName}, Base: {baseObject}");
        }
    }

    /// <summary>
    /// Generate CREATE TABLE AS statement
    /// </summary>
    public string GenerateCreateTableAs(string tableName, string baseObject, Expression linqExpression)
    {
        ModelCreatingScope.EnsureInScope();
        try
        {
            var context = new QueryAssemblyContext(baseObject, false); // Push Query
            var structure = CreateTableAsStructure(tableName, baseObject);

            // Analyze LINQ expression to build query clauses
            structure = ProcessLinqExpression(structure, linqExpression, context);

            var query = AssembleStructuredQuery(structure);
            return ApplyQueryPostProcessing(query, context);
        }
        catch (Exception ex)
        {
            return HandleGenerationError("CREATE TABLE AS generation", ex, $"Table: {tableName}, Base: {baseObject}");
        }
    }

    /// <summary>
    /// Generate column definitions
    /// </summary>
    private string GenerateColumnDefinitions(DdlSchemaDefinition schema, bool isStream)
    {
        var keyColumns = schema.Columns.Where(c => c.IsKey).ToList();
        var nonKeyColumns = schema.Columns.Where(c => !c.IsKey).ToList();

        var columns = new List<string>();

        if (keyColumns.Count > 1 && !isStream)
        {
            var fields = keyColumns.Select(c => $"{QuoteIfReserved(c.Name)} {c.Type}");
            var structDef = $"STRUCT<{string.Join(", ", fields)}>";
            var keyColumnName = $"{schema.ObjectName}_key";
            columns.Add($"{keyColumnName} {structDef} {(isStream ? "KEY" : "PRIMARY KEY")}");
            columns.AddRange(nonKeyColumns.Select(c => $"{QuoteIfReserved(c.Name)} {c.Type}"));
        }
        else
        {
            foreach (var column in schema.Columns)
            {
                var cname = QuoteIfReserved(column.Name);
                var definition = $"{cname} {column.Type}";
                if (column.IsKey)
                    definition += isStream ? " KEY" : " PRIMARY KEY";
                columns.Add(definition);
            }
        }

        return string.Join(", ", columns);
    }

    private static string QuoteIfReserved(string name)
    {
        // Minimal reserved set needed by our schemas
        switch (name)
        {
            case "Topic":
            case "Partition":
            case "Offset":
                return $"`{name}`";
            default:
                return name;
        }
    }

    /// <summary>
    /// Create CREATE STREAM AS structure
    /// </summary>
    private static QueryStructure CreateStreamAsStructure(string streamName, string baseObject)
    {
        var metadata = new QueryMetadata(DateTime.UtcNow, "DDL", baseObject);
        var structure = QueryStructure.CreateStreamAs(streamName, baseObject).WithMetadata(metadata);
        var fromClause = QueryClause.Required(QueryClauseType.From, $"FROM {baseObject}");
        return structure.AddClause(fromClause);
    }

    /// <summary>
    /// Create CREATE TABLE AS structure
    /// </summary>
    private static QueryStructure CreateTableAsStructure(string tableName, string baseObject)
    {
        var metadata = new QueryMetadata(DateTime.UtcNow, "DDL", baseObject);
        var structure = QueryStructure.CreateTableAs(tableName, baseObject).WithMetadata(metadata);
        var fromClause = QueryClause.Required(QueryClauseType.From, $"FROM {baseObject}");
        return structure.AddClause(fromClause);
    }

    /// <summary>
    /// Process LINQ expression
    /// </summary>
    private QueryStructure ProcessLinqExpression(QueryStructure structure, Expression linqExpression, QueryAssemblyContext context)
    {
        var analysis = AnalyzeLinqExpression(linqExpression);
        var metadata = structure.Metadata;
        var analysisMd = analysis.ToMetadata();
        if (analysisMd.Properties is { } props)
        {
            foreach (var kv in props)
                metadata = metadata.WithProperty(kv.Key, kv.Value);
        }
        structure = structure.WithMetadata(metadata);

        foreach (var methodCall in analysis.MethodCalls.AsEnumerable().Reverse())
        {
            structure = ProcessMethodCall(structure, methodCall, context);
        }

        return structure;
    }

    /// <summary>
    /// Process method call
    /// </summary>
    private QueryStructure ProcessMethodCall(QueryStructure structure, MethodCallExpression methodCall, QueryAssemblyContext context)
    {
        var methodName = methodCall.Method.Name;

        return methodName switch
        {
            "Select" => ProcessSelectMethod(structure, methodCall),
            "Where" => ProcessWhereMethod(structure, methodCall),
            "GroupBy" => ProcessGroupByMethod(structure, methodCall),
            "Join" => ProcessJoinMethod(structure, methodCall),
            "Having" => ProcessHavingMethod(structure, methodCall),
            "OrderBy" or "OrderByDescending" or "ThenBy" or "ThenByDescending" => ProcessOrderByMethod(structure, methodCall),
            _ => structure // Ignore unsupported methods
        };
    }

    /// <summary>
    /// Handle SELECT method
    /// </summary>
    private QueryStructure ProcessSelectMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (methodCall.Arguments.Count >= 2)
        {
            var lambdaBody = ExtractLambdaBody(methodCall.Arguments[1]);
            if (lambdaBody != null)
            {
                var selectContent = SafeCallBuilder(KsqlBuilderType.Select, lambdaBody, "SELECT processing");
                var clause = QueryClause.Required(QueryClauseType.Select, selectContent, lambdaBody);
                structure = structure.AddClause(clause);
            }
        }

        return structure;
    }

    /// <summary>
    /// Handle WHERE method
    /// </summary>
    private QueryStructure ProcessWhereMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (methodCall.Arguments.Count >= 2)
        {
            var lambdaBody = ExtractLambdaBody(methodCall.Arguments[1]);
            if (lambdaBody != null)
            {
                var whereContent = SafeCallBuilder(KsqlBuilderType.Where, lambdaBody, "WHERE processing");
                var clause = QueryClause.Required(QueryClauseType.Where, $"WHERE {whereContent}", lambdaBody);
                structure = structure.AddClause(clause);
            }
        }

        return structure;
    }

    /// <summary>
    /// Handle GROUP BY method
    /// </summary>
    private QueryStructure ProcessGroupByMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (methodCall.Arguments.Count >= 2)
        {
            var lambdaBody = ExtractLambdaBody(methodCall.Arguments[1]);
            if (lambdaBody != null)
            {
                var groupByContent = SafeCallBuilder(KsqlBuilderType.GroupBy, lambdaBody, "GROUP BY processing");
                var clause = QueryClause.Required(QueryClauseType.GroupBy, $"GROUP BY {groupByContent}", lambdaBody);
                structure = structure.AddClause(clause);
            }
        }

        return structure;
    }


    /// <summary>
    /// Handle HAVING method
    /// </summary>
    private QueryStructure ProcessHavingMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (HasBuilder(KsqlBuilderType.Having) && methodCall.Arguments.Count >= 2)
        {
            var lambdaBody = ExtractLambdaBody(methodCall.Arguments[1]);
            if (lambdaBody != null)
            {
                var havingContent = SafeCallBuilder(KsqlBuilderType.Having, lambdaBody, "HAVING processing");
                var clause = QueryClause.Required(QueryClauseType.Having, $"HAVING {havingContent}", lambdaBody);
                structure = structure.AddClause(clause);
            }
        }

        return structure;
    }

    /// <summary>
    /// Handle ORDER BY method
    /// </summary>
    private QueryStructure ProcessOrderByMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (HasBuilder(KsqlBuilderType.OrderBy))
        {
            var orderByContent = SafeCallBuilder(KsqlBuilderType.OrderBy, methodCall, "ORDER BY processing");
            var clause = QueryClause.Optional(QueryClauseType.OrderBy, $"ORDER BY {orderByContent}", methodCall);
            structure = structure.AddClause(clause);
        }

        return structure;
    }

    /// <summary>
    /// Handle JOIN method (supports simple inner JOIN only)
    /// </summary>
    private QueryStructure ProcessJoinMethod(QueryStructure structure, MethodCallExpression methodCall)
    {
        if (HasBuilder(KsqlBuilderType.Join))
        {
            var joinContent = SafeCallBuilder(KsqlBuilderType.Join, methodCall, "JOIN processing");
            var fromIndex = joinContent.IndexOf("FROM", StringComparison.OrdinalIgnoreCase);
            if (fromIndex >= 0)
            {
                var fromPart = joinContent.Substring(fromIndex); // FROM table JOIN ...
                var clause = QueryClause.Required(QueryClauseType.From, fromPart, methodCall);
                structure = structure.RemoveClause(QueryClauseType.From);
                structure = structure.AddClause(clause);
            }
        }

        return structure;
    }

    /// <summary>
    /// Analyze LINQ expression
    /// </summary>
    private ExpressionAnalysisResult AnalyzeLinqExpression(Expression expression)
    {
        var visitor = new MethodCallCollectorVisitor();
        visitor.Visit(expression);
        var result = visitor.Result;

        var selectCall = result.MethodCalls.FirstOrDefault(mc => mc.Method.Name == "Select");
        if (selectCall != null && selectCall.Arguments.Count > 1)
        {
            var body = ExtractLambdaBody(selectCall.Arguments[1]);
            if (body != null)
            {
                var ws = new WindowStartDetectionVisitor();
                ws.Visit(body);
                result.WindowStartCallCount = ws.Count;
                result.BucketColumnName = ws.ColumnName;
            }
        }

        var type = expression.Type.IsGenericType ? expression.Type.GetGenericArguments().FirstOrDefault() : null;
        if (type != null)
            result.PocoType = type;
        if (result.Windows.Count > 0)
        {
            if (result.TimeKey == null)
                throw new InvalidOperationException("Time key is required");
            if (string.IsNullOrEmpty(result.BucketColumnName))
                throw new InvalidOperationException("WindowStart() projection required for windowed queries");
            if (result.WindowStartCallCount != 1)
                throw new InvalidOperationException("Windowed query requires exactly one WindowStart() in projection.");
        }
        WindowValidator.Validate(result);
        return result;
    }

    /// <summary>
    /// Extract lambda body
    /// </summary>
    private static Expression? ExtractLambdaBody(Expression expression)
    {
        return BuilderValidation.ExtractLambdaBody(expression);
    }

    /// <summary>
    /// Create standard builders
    /// </summary>
    private static IReadOnlyDictionary<KsqlBuilderType, IKsqlBuilder> CreateStandardBuilders()
    {
        return new Dictionary<KsqlBuilderType, IKsqlBuilder>
        {
            [KsqlBuilderType.Select] = new SelectClauseBuilder(),
            [KsqlBuilderType.Where] = new WhereClauseBuilder(),
            [KsqlBuilderType.GroupBy] = new GroupByClauseBuilder(),
            [KsqlBuilderType.Having] = new HavingClauseBuilder(),
            [KsqlBuilderType.Join] = new JoinClauseBuilder(),
        };
    }
}
