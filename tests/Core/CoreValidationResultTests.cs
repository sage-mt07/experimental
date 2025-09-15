using System;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Core;

public class CoreValidationResultTests
{
    private static Type GetTypeRef()
    {
        var asm = typeof(Kafka.Ksql.Linq.Core.Abstractions.ValidationResult).Assembly;
        return asm.GetType("Kafka.Ksql.Linq.Core.Validation.CoreValidationResult")!;
    }

    [Fact(Skip = "CoreValidationResult not available")]
    public void Properties_CanBeSetViaReflection()
    {
        var t = GetTypeRef();
        var obj = Activator.CreateInstance(t)!;
        t.GetProperty("IsValid")!.SetValue(obj, true);
        t.GetProperty("Errors")!.SetValue(obj, new System.Collections.Generic.List<string> { "e" });
        t.GetProperty("Warnings")!.SetValue(obj, new System.Collections.Generic.List<string> { "w" });
        Assert.True((bool)t.GetProperty("IsValid")!.GetValue(obj)!);
        var errs = (System.Collections.Generic.List<string>)t.GetProperty("Errors")!.GetValue(obj)!;
        Assert.Single(errs);
        var warns = (System.Collections.Generic.List<string>)t.GetProperty("Warnings")!.GetValue(obj)!;
        Assert.Single(warns);
    }
}
