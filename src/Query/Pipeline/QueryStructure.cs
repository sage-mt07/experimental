using System;
using System.Collections.Generic;
using System.Linq;

namespace Kafka.Ksql.Linq.Query.Pipeline;

/// <summary>
/// Query structure definition.
/// Rationale: unify query structure management in the generator layer.
/// </summary>
internal record QueryStructure(
    string QueryType,
    string TargetObject,
    List<QueryClause> Clauses,
    QueryMetadata Metadata)
{
    /// <summary>
    /// Create SELECT structure.
    /// </summary>
    public static QueryStructure CreateSelect(string targetObject)
    {
        return new QueryStructure(
            "SELECT",
            targetObject,
            new List<QueryClause>(),
            new QueryMetadata(DateTime.UtcNow, "DML"));
    }

    /// <summary>
    /// Create CREATE STREAM AS structure.
    /// </summary>
    public static QueryStructure CreateStreamAs(string streamName, string baseObject)
    {
        return new QueryStructure(
            "CREATE_STREAM_AS",
            streamName,
            new List<QueryClause>(),
            new QueryMetadata(DateTime.UtcNow, "DDL", baseObject));
    }

    /// <summary>
    /// Create CREATE TABLE AS structure.
    /// </summary>
    public static QueryStructure CreateTableAs(string tableName, string baseObject)
    {
        return new QueryStructure(
            "CREATE_TABLE_AS",
            tableName,
            new List<QueryClause>(),
            new QueryMetadata(DateTime.UtcNow, "DDL", baseObject));
    }

    /// <summary>
    /// Add a clause.
    /// </summary>
    private static readonly QueryClauseType[] ClauseInsertionOrder = new[]
    {
        QueryClauseType.Select,
        QueryClauseType.From,
        QueryClauseType.Join,
        QueryClauseType.Where,
        QueryClauseType.GroupBy,
        QueryClauseType.Having,
        QueryClauseType.OrderBy,
        QueryClauseType.Limit
    };

    public QueryStructure AddClause(QueryClause clause)
    {
        var newClauses = new List<QueryClause>(Clauses);
        int insertIndex = newClauses.FindIndex(c =>
            Array.IndexOf(ClauseInsertionOrder, clause.Type) <
            Array.IndexOf(ClauseInsertionOrder, c.Type));

        if (insertIndex >= 0)
        {
            newClauses.Insert(insertIndex, clause);
        }
        else
        {
            newClauses.Add(clause);
        }

        return this with { Clauses = newClauses };
    }

    /// <summary>
    /// Add multiple clauses.
    /// </summary>
    public QueryStructure AddClauses(params QueryClause[] clauses)
    {
        var structure = this;
        foreach (var clause in clauses)
        {
            structure = structure.AddClause(clause);
        }
        return structure;
    }

    /// <summary>
    /// Get a clause by type.
    /// </summary>
    public QueryClause? GetClause(QueryClauseType type)
    {
        return Clauses.FirstOrDefault(c => c.Type == type);
    }

    /// <summary>
    /// Check if a clause exists.
    /// </summary>
    public bool HasClause(QueryClauseType type)
    {
        return Clauses.Any(c => c.Type == type);
    }

    /// <summary>
    /// Remove a clause.
    /// </summary>
    public QueryStructure RemoveClause(QueryClauseType type)
    {
        var newClauses = Clauses.Where(c => c.Type != type).ToList();
        return this with { Clauses = newClauses };
    }

    /// <summary>
    /// Update metadata.
    /// </summary>
    public QueryStructure WithMetadata(QueryMetadata metadata)
    {
        return this with { Metadata = metadata };
    }

    /// <summary>
    /// Validate structure.
    /// </summary>
    public ValidationResult Validate()
    {
        var errors = new List<string>();

        // Basic validation
        if (string.IsNullOrWhiteSpace(TargetObject))
        {
            errors.Add("Target object is required");
        }

        // Validation based on query type
        switch (QueryType)
        {
            case "SELECT":
                ValidateSelectStructure(errors);
                break;
            case "CREATE_STREAM_AS":
            case "CREATE_TABLE_AS":
                ValidateCreateAsStructure(errors);
                break;
        }

        // Validate clause order
        ValidateClauseOrder(errors);

        return new ValidationResult(errors.Count == 0, errors);
    }

    /// <summary>
    /// Validate SELECT structure
    /// </summary>
    private void ValidateSelectStructure(List<string> errors)
    {
        // SELECT queries have no required clauses (SELECT * by default)
        // However, aggregation is required when GROUP BY is present
        if (HasClause(QueryClauseType.GroupBy) && !HasClause(QueryClauseType.Select))
        {
            errors.Add("GROUP BY requires explicit SELECT clause with aggregations");
        }
    }

    /// <summary>
    /// Validate CREATE AS structure
    /// </summary>
    private void ValidateCreateAsStructure(List<string> errors)
    {
        if (string.IsNullOrWhiteSpace(Metadata.BaseObject))
        {
            errors.Add("CREATE AS requires base object in metadata");
        }

        // For CREATE TABLE, GROUP BY or windowing is recommended
        if (QueryType == "CREATE_TABLE_AS" &&
            !HasClause(QueryClauseType.GroupBy))
        {
            // Warning level (not an error)
        }
    }

    /// <summary>
    /// Validate clause order
    /// </summary>
    private void ValidateClauseOrder(List<string> errors)
    {
        var expectedOrder = new[]
        {
            QueryClauseType.Select,
            QueryClauseType.From,
            QueryClauseType.Join,
            QueryClauseType.Where,
            QueryClauseType.GroupBy,
            QueryClauseType.Having,
            QueryClauseType.OrderBy,
            QueryClauseType.Limit
        };

        var actualOrder = Clauses.Select(c => c.Type).ToList();
        var lastValidIndex = -1;

        foreach (var clauseType in actualOrder)
        {
            var expectedIndex = Array.IndexOf(expectedOrder, clauseType);
            if (expectedIndex <= lastValidIndex)
            {
                errors.Add($"Clause {clauseType} is in incorrect order");
            }
            lastValidIndex = Math.Max(lastValidIndex, expectedIndex);
        }
    }
}
