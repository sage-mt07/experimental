using System;
using System.Collections.Generic;
using System.Linq;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Kafka.Ksql.Linq.Tests;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method | AttributeTargets.Assembly, AllowMultiple = true)]
[TraitDiscoverer("Kafka.Ksql.Linq.Tests.CategoryDiscoverer", "Kafka.Ksql.Linq.Tests")]
public sealed class CategoryAttribute : Attribute, ITraitAttribute
{
    public string Category { get; }
    public CategoryAttribute(string category) => Category = category;
}

public sealed class CategoryDiscoverer : ITraitDiscoverer
{
    public IEnumerable<KeyValuePair<string, string>> GetTraits(IAttributeInfo traitAttribute)
    {
        var category = traitAttribute.GetConstructorArguments().FirstOrDefault()?.ToString() ?? string.Empty;
        yield return new KeyValuePair<string, string>("Category", category);
    }
}
