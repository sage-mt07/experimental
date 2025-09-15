using System;
using System.Linq;
using System.Linq.Expressions;
using Kafka.Ksql.Linq.Query.Dsl;

namespace Kafka.Ksql.Linq.Query.Pipeline;

internal class MethodCallCollectorVisitor : ExpressionVisitor
{
    public ExpressionAnalysisResult Result { get; } = new();

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        Result.MethodCalls.Add(node);
        switch (node.Method.Name)
        {
            case "Tumbling":
                ParseTumbling(node);
                break;
            case "GroupBy":
                ParseGroupBy(node);
                break;
            case "TimeFrame":
                ParseTimeFrame(node);
                break;
            case "WhenEmpty":
                Result.WhenEmpty = true;
                break;
        }
        return base.VisitMethodCall(node);
    }

    private void ParseTumbling(MethodCallExpression call)
    {
        if (call.Arguments[0] is UnaryExpression ue && ue.Operand is LambdaExpression le && le.Body is MemberExpression me)
            Result.TimeKey = me.Member.Name;

        if (call.Arguments.Count > 1)
        {
            var arg = call.Arguments[1];
            if (arg is MemberInitExpression mie)
            {
                foreach (var binding in mie.Bindings.OfType<MemberAssignment>())
                {
                    var name = binding.Member.Name;
                    var expr = binding.Expression;
                    if (expr is NewArrayExpression nae)
                    {
                        foreach (var ce in nae.Expressions.OfType<ConstantExpression>())
                            Result.Windows.Add(Normalize((int)ce.Value!, name));
                    }
                    else if (expr is ConstantExpression ce && ce.Value is int[] arr)
                    {
                        foreach (var v in arr)
                            Result.Windows.Add(Normalize(v, name));
                    }
                }
            }
            else if (arg is ConstantExpression ce && ce.Value is Windows w)
            {
                if (w.Minutes != null) foreach (var v in w.Minutes) Result.Windows.Add(Normalize(v, nameof(w.Minutes)));
                if (w.Hours != null) foreach (var v in w.Hours) Result.Windows.Add(Normalize(v, nameof(w.Hours)));
                if (w.Days != null) foreach (var v in w.Days) Result.Windows.Add(Normalize(v, nameof(w.Days)));
                if (w.Months != null) foreach (var v in w.Months) Result.Windows.Add(Normalize(v, nameof(w.Months)));
            }
        }

        if (call.Arguments.Count > 2 && call.Arguments[2] is ConstantExpression ce2 && ce2.Value is int b)
            Result.BaseUnitSeconds = b;

        if (call.Arguments.Count > 3 && call.Arguments[3] is ConstantExpression ce3 && ce3.Value is TimeSpan ts)
            Result.GraceSeconds = (int)Math.Ceiling(ts.TotalSeconds);

        var ordered = Result.Windows.Distinct().OrderBy(ToMinutes).ToList();
        Result.Windows.Clear();
        Result.Windows.AddRange(ordered);
    }

    private static string Normalize(int value, string unit) => unit.ToLowerInvariant() switch
    {
        "minutes" => value + "m",
        "hours" => value + "h",
        "days" => value + "d",
        "months" => value + "mo",
        _ => value.ToString()
    };

    private static int ToMinutes(string tf)
    {
        if (tf.EndsWith("mo")) return int.Parse(tf[..^2]) * 43200;
        if (tf.EndsWith("wk")) return int.Parse(tf[..^2]) * 10080;
        var unit = tf[^1];
        var val = int.Parse(tf[..^1]);
        return unit switch
        {
            'm' => val,
            'h' => val * 60,
            'd' => val * 1440,
            _ => val
        };
    }

    private void ParseGroupBy(MethodCallExpression call)
    {
        if (call.Arguments.Count > 0)
        {
            var arg = call.Arguments[0];
            if (arg is UnaryExpression ue && ue.Operand is LambdaExpression le && le.Body is NewExpression ne)
            {
                foreach (var m in ne.Members!)
                    Result.GroupByKeys.Add(m.Name);
            }
            else if (arg is LambdaExpression le2 && le2.Body is NewExpression ne2)
            {
                foreach (var m in ne2.Members!)
                    Result.GroupByKeys.Add(m.Name);
            }
        }
    }

    private void ParseTimeFrame(MethodCallExpression call)
    {
        if (call.Arguments.Count == 0) return;
        if (call.Arguments[0] is UnaryExpression ue && ue.Operand is LambdaExpression le1)
        {
            TraverseBasedOn(le1.Body);
        }
        else if (call.Arguments[0] is LambdaExpression le2)
        {
            TraverseBasedOn(le2.Body);
        }

        if (call.Arguments.Count > 1)
        {
            var arg = call.Arguments[1];
            if (arg is UnaryExpression u && u.Operand is LambdaExpression leDay)
            {
                if (leDay.Body is MemberExpression me)
                    Result.BasedOnDayKey = me.Member.Name;
                else if (leDay.Body is UnaryExpression ce && ce.Operand is MemberExpression me2)
                    Result.BasedOnDayKey = me2.Member.Name;
            }
            else if (arg is LambdaExpression leDay2)
            {
                if (leDay2.Body is MemberExpression me3)
                    Result.BasedOnDayKey = me3.Member.Name;
                else if (leDay2.Body is UnaryExpression ce2 && ce2.Operand is MemberExpression me4)
                    Result.BasedOnDayKey = me4.Member.Name;
            }
        }
    }

    private void TraverseBasedOn(Expression expr)
    {
        switch (expr)
        {
            case BinaryExpression be when be.NodeType == ExpressionType.AndAlso:
                TraverseBasedOn(be.Left);
                TraverseBasedOn(be.Right);
                break;
            case BinaryExpression be when be.NodeType == ExpressionType.Equal:
                if (be.Left is MemberExpression lm && be.Right is MemberExpression rm)
                    Result.BasedOnJoinKeys.Add(lm.Member.Name);
                break;
            case BinaryExpression be when be.NodeType == ExpressionType.LessThan || be.NodeType == ExpressionType.LessThanOrEqual:
                HandleTimeComparison(be.Left, be.Right, be.NodeType);
                HandleTimeComparison(be.Right, be.Left, be.NodeType);
                break;
            case BinaryExpression be when be.NodeType == ExpressionType.GreaterThan || be.NodeType == ExpressionType.GreaterThanOrEqual:
                var mapped = be.NodeType == ExpressionType.GreaterThanOrEqual ? ExpressionType.LessThanOrEqual : ExpressionType.LessThan;
                HandleTimeComparison(be.Left, be.Right, mapped);
                HandleTimeComparison(be.Right, be.Left, mapped);
                break;
        }
    }

    private void HandleTimeComparison(Expression scheduleExpr, Expression timeExpr, ExpressionType nodeType)
    {
        if (scheduleExpr is MemberExpression sm && timeExpr is MemberExpression tm)
        {
            if (nodeType == ExpressionType.LessThanOrEqual)
            {
                Result.BasedOnOpen = sm.Member.Name;
                Result.BasedOnOpenInclusive = true;
            }
            else if (nodeType == ExpressionType.LessThan)
            {
                Result.BasedOnClose = sm.Member.Name;
                Result.BasedOnCloseInclusive = false;
            }
        }
    }
}
