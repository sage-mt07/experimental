using System;
using System.Collections.Generic;
using System.Linq;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Kafka.Ksql.Linq.Tests.Integration;

/// <summary>
/// Specifies the execution order for a test case.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public sealed class TestPriorityAttribute : Attribute
{
    public TestPriorityAttribute(int priority) => Priority = priority;
    public int Priority { get; }
}

/// <summary>
/// Orders test cases by the <see cref="TestPriorityAttribute"/> value.
/// </summary>
public sealed class PriorityOrderer : ITestCaseOrderer
{
    public IEnumerable<TTestCase> OrderTestCases<TTestCase>(IEnumerable<TTestCase> testCases) where TTestCase : ITestCase
    {
        var sorted = testCases.OrderBy(tc =>
        {
            var attr = tc.TestMethod.Method.GetCustomAttributes(typeof(TestPriorityAttribute).AssemblyQualifiedName!).FirstOrDefault();
            return attr == null ? 0 : attr.GetNamedArgument<int>("priority");
        });
        return sorted;
    }
}

