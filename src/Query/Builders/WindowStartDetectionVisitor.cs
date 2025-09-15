using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Builders;

internal class WindowStartDetectionVisitor : ExpressionVisitor
{
    public int Count { get; private set; }
    public string? ColumnName { get; private set; }

    protected override Expression VisitNew(NewExpression node)
    {
        if (node.Members != null)
        {
            for (int i = 0; i < node.Arguments.Count; i++)
            {
                if (node.Arguments[i] is MethodCallExpression mc && mc.Method.Name == "WindowStart")
                {
                    Count++;
                    ColumnName = node.Members[i].Name;
                }
                else
                {
                    Visit(node.Arguments[i]);
                }
            }
            return node;
        }
        return base.VisitNew(node);
    }

    protected override MemberAssignment VisitMemberAssignment(MemberAssignment node)
    {
        if (node.Expression is MethodCallExpression mc && mc.Method.Name == "WindowStart")
        {
            Count++;
            ColumnName = node.Member.Name;
            return node;
        }
        return base.VisitMemberAssignment(node);
    }

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.Name == "WindowStart")
            Count++;
        return base.VisitMethodCall(node);
    }
}
