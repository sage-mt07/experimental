using Kafka.Ksql.Linq.Query.Dsl;
using Kafka.Ksql.Linq.Query.Pipeline;
using System;
using System.Linq;
using System.Linq.Expressions;
using Xunit;

namespace Kafka.Ksql.Linq.Tests.Query.Dsl;

public class PropertyNameParsingTests
{
    private class Rate
    {
        public int Id { get; set; }
        public DateTime Timestamp { get; set; }
    }

    private class Schedule
    {
        public int Id { get; set; }
        public DateTime Open { get; set; }
        public DateTime Close { get; set; }
        public DateTime Day { get; set; }
    }

    [Fact]
    public void Tumbling_Extracts_TimeKey()
    {
        var q = Expression.Parameter(typeof(KsqlQueryable<Rate>), "q");
        var r = Expression.Parameter(typeof(Rate), "r");
        var timeLambda = Expression.Lambda(Expression.Property(r, nameof(Rate.Timestamp)), r);
        var method = typeof(KsqlQueryable<Rate>).GetMethods().First(m => m.Name == "Tumbling" && m.GetParameters().Length == 4);
        var windows = Expression.MemberInit(Expression.New(typeof(Windows)));
        var call = Expression.Call(q, method,
            timeLambda,
            windows,
            Expression.Constant(10),
            Expression.Constant(null, typeof(TimeSpan?)));
        var visitor = new MethodCallCollectorVisitor();
        visitor.Visit(call);
        Assert.Equal("Timestamp", visitor.Result.TimeKey);
    }

    [Fact]
    public void Tumbling_Sets_TimeKey_On_Model()
    {
        var model = new KsqlQueryable<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Minutes = new[] { 1 } })
            .Build();
        Assert.Equal("Timestamp", model.TimeKey);
    }

    [Fact]
    public void TimeFrame_Extracts_DayKey()
    {
        var q = Expression.Parameter(typeof(KsqlQueryable<Rate>), "q");
        var r = Expression.Parameter(typeof(Rate), "r");
        var s = Expression.Parameter(typeof(Schedule), "s");
        var predicate = Expression.Lambda(
            Expression.AndAlso(
                Expression.Equal(Expression.Property(r, nameof(Rate.Id)), Expression.Property(s, nameof(Schedule.Id))),
                Expression.AndAlso(
                    Expression.LessThanOrEqual(Expression.Property(s, nameof(Schedule.Open)), Expression.Property(r, nameof(Rate.Timestamp))),
                    Expression.LessThan(Expression.Property(r, nameof(Rate.Timestamp)), Expression.Property(s, nameof(Schedule.Close))))),
            r, s);
        var dayLambda = Expression.Lambda(
            Expression.Convert(Expression.Property(s, nameof(Schedule.Day)), typeof(object)), s);
        var method = typeof(KsqlQueryable<Rate>).GetMethods().First(m => m.Name == "TimeFrame" && m.GetParameters().Length == 2);
        var generic = method.MakeGenericMethod(typeof(Schedule));
        var call = Expression.Call(q, generic, predicate, dayLambda);
        var visitor = new MethodCallCollectorVisitor();
        visitor.Visit(call);
        Assert.Equal("Day", visitor.Result.BasedOnDayKey);
    }

    [Fact]
    public void Tumbling2_Extracts_TimeKey()
    {
        var q = Expression.Parameter(typeof(KsqlQueryable2<Rate, Schedule>), "q");
        var r = Expression.Parameter(typeof(Rate), "r");
        var s = Expression.Parameter(typeof(Schedule), "s");
        var timeLambda = Expression.Lambda(Expression.Property(r, nameof(Rate.Timestamp)), r, s);
        var method = typeof(KsqlQueryable2<Rate, Schedule>).GetMethods()
            .First(m => m.Name == "Tumbling" && m.GetParameters().Length == 4);
        var windows = Expression.MemberInit(Expression.New(typeof(Windows)));
        var call = Expression.Call(q, method,
            timeLambda,
            windows,
            Expression.Constant(10),
            Expression.Constant(null, typeof(TimeSpan?)));
        var visitor = new MethodCallCollectorVisitor();
        visitor.Visit(call);
        Assert.Equal("Timestamp", visitor.Result.TimeKey);
    }

    [Fact]
    public void Tumbling2_Sets_TimeKey_And_Orders_Windows()
    {
        var model = new KsqlQueryable2<Rate, Schedule>()
            .Tumbling((r, s) => r.Timestamp, new Windows { Minutes = new[] { 90, 15, 15 }, Hours = new[] { 1 } })
            .Build();
        Assert.Equal("Timestamp", model.TimeKey);
        Assert.Equal(new[] { "15m", "1h", "90m" }, model.Windows);
    }

    [Fact]
    public void Tumbling_Orders_Month_Windows()
    {
        var model = new KsqlQueryable<Rate>()
            .Tumbling(r => r.Timestamp, new Windows { Days = new[] { 1 }, Months = new[] { 6, 1 } })
            .Build();
        Assert.Equal(new[] { "1d", "1mo", "6mo" }, model.Windows);
    }

    [Fact]
    public void WhenEmpty_Sets_Flag()
    {
        var q = Expression.Parameter(typeof(KsqlQueryable<Rate>), "q");
        var r1 = Expression.Parameter(typeof(Rate), "r1");
        var r2 = Expression.Parameter(typeof(Rate), "r2");
        var filler = Expression.Lambda<Func<Rate, Rate, Rate>>(r1, r1, r2);
        var method = typeof(KsqlQueryable<Rate>).GetMethod("WhenEmpty");
        var call = Expression.Call(q, method!, filler);
        var visitor = new MethodCallCollectorVisitor();
        visitor.Visit(call);
        Assert.True(visitor.Result.WhenEmpty);
    }
}

