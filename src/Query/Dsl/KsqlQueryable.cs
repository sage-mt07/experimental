using System;
using System.Linq;
using System.Linq.Expressions;

namespace Kafka.Ksql.Linq.Query.Dsl;

/// <summary>
/// Represents a queryable with a single source type.
/// Provides chaining methods for WHERE, SELECT, WINDOW and JOIN operations.
/// </summary>
public class KsqlQueryable<T1> : IKsqlQueryable, IScheduledScope<T1>
{
    private readonly KsqlQueryModel _model = new()
    {
        SourceTypes = new[] { typeof(T1) }
    };
    private QueryBuildStage _stage = QueryBuildStage.From;

    public KsqlQueryable<T1> Where(Expression<Func<T1, bool>> predicate)
    {
        if (_stage is QueryBuildStage.Select or QueryBuildStage.GroupBy or QueryBuildStage.Having)
            throw new InvalidOperationException("Where() must be called before GroupBy/Having/Select().");

        _model.WhereCondition = predicate;
        _stage = QueryBuildStage.Where;
        return this;
    }

    public KsqlQueryable<T1> Select<TResult>(Expression<Func<T1, TResult>> projection)
    {
        if (_stage == QueryBuildStage.Select)
            throw new InvalidOperationException("Select() has already been specified.");

        if (_stage == QueryBuildStage.Join || _stage == QueryBuildStage.From || _stage == QueryBuildStage.Where || _stage == QueryBuildStage.GroupBy || _stage == QueryBuildStage.Having)
        {
            _stage = QueryBuildStage.Select;
        }
        else
        {
            throw new InvalidOperationException("Select() cannot be called in the current state.");
        }

        _model.SelectProjection = projection;
        var visitor = new Kafka.Ksql.Linq.Query.Builders.AggregateDetectionVisitor();
        visitor.Visit(projection.Body);
        var wsVisitor = new Kafka.Ksql.Linq.Query.Builders.WindowStartDetectionVisitor();
        wsVisitor.Visit(projection.Body);
        _model.BucketColumnName = wsVisitor.ColumnName;
        return this;
    }

    public KsqlGroupedQueryable<T1, TKey> GroupBy<TKey>(Expression<Func<T1, TKey>> keySelector)
    {
        if (_stage == QueryBuildStage.Select)
            throw new InvalidOperationException("GroupBy() must be called before Select().");

        _model.GroupByExpression = keySelector;
        _stage = QueryBuildStage.GroupBy;
        return new KsqlGroupedQueryable<T1, TKey>(_model);
    }

    public KsqlQueryable<T1> Tumbling(
        Expression<Func<T1, DateTime>> time,
        Windows windows,
        int baseUnitSeconds = 10,
        TimeSpan? grace = null)
    {
        if (time.Body is MemberExpression me)
            _model.TimeKey = me.Member.Name;
        else if (time.Body is UnaryExpression ue && ue.Operand is MemberExpression me2)
            _model.TimeKey = me2.Member.Name;
        if (windows.Minutes != null) foreach (var m in windows.Minutes) _model.Windows.Add($"{m}m");
        if (windows.Hours != null) foreach (var h in windows.Hours) _model.Windows.Add($"{h}h");
        if (windows.Days != null) foreach (var d in windows.Days) _model.Windows.Add($"{d}d");
        if (windows.Months != null) foreach (var mo in windows.Months) _model.Windows.Add($"{mo}mo");
        _model.BaseUnitSeconds = baseUnitSeconds;
        if (grace.HasValue)
            _model.GraceSeconds = (int)Math.Ceiling(grace.Value.TotalSeconds);
        static int ToMinutes(string tf)
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
        var ordered = _model.Windows.Distinct().OrderBy(ToMinutes).ToList();
        _model.Windows.Clear();
        _model.Windows.AddRange(ordered);
        return this;
    }

    public KsqlQueryable<T1> Tumbling(Expression<Func<T1, object>> timeProperty, TimeSpan size)
    {
        throw new NotSupportedException("Legacy Tumbling overload is not supported in this phase.");
    }

    // Filtering raw is upstream's responsibility. The DSL references an already-filtered stream
    // (e.g., trade_raw_filtered)
    public IScheduledScope<T1> TimeFrame<TSchedule>(
        Expression<Func<T1, TSchedule, bool>> predicate,
        Expression<Func<TSchedule, object>>? dayKey = null)
    {
        _model.BasedOnType = typeof(TSchedule);
        _model.BasedOnDayKey = dayKey;
        Parse(predicate.Body);
        return this;

        void Parse(Expression expr)
        {
            switch (expr)
            {
                case BinaryExpression be when be.NodeType == ExpressionType.AndAlso:
                    Parse(be.Left);
                    Parse(be.Right);
                    break;
                case BinaryExpression be when be.NodeType == ExpressionType.Equal:
                    if (be.Left is MemberExpression lm && be.Right is MemberExpression rm)
                    {
                        if (lm.Expression is ParameterExpression lp && lp.Type == typeof(T1) &&
                            rm.Expression is ParameterExpression rp && rp.Type == typeof(TSchedule))
                            _model.BasedOnJoinKeys.Add(lm.Member.Name);
                        else if (rm.Expression is ParameterExpression rp2 && rp2.Type == typeof(T1) &&
                                 lm.Expression is ParameterExpression lp2 && lp2.Type == typeof(TSchedule))
                            _model.BasedOnJoinKeys.Add(rm.Member.Name);
                    }
                    break;
                case BinaryExpression be when be.NodeType is ExpressionType.LessThan or ExpressionType.LessThanOrEqual or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual:
                    HandleBoundary(be);
                    break;
            }
        }

        void HandleBoundary(BinaryExpression be)
        {
            if (be.Left is not MemberExpression l || be.Right is not MemberExpression r) return;
            if (l.Expression is not ParameterExpression lp || r.Expression is not ParameterExpression rp) return;
            var lSched = lp.Type == typeof(TSchedule);
            var rSched = rp.Type == typeof(TSchedule);
            if (lSched == rSched) return;
            var schedProp = lSched ? l.Member.Name : r.Member.Name;
            var inclusive = be.NodeType is ExpressionType.LessThanOrEqual or ExpressionType.GreaterThanOrEqual;
            var isOpen = lSched
                ? be.NodeType is ExpressionType.LessThan or ExpressionType.LessThanOrEqual
                : be.NodeType is ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual;
            if (isOpen)
            {
                _model.BasedOnOpen = schedProp;
                _model.BasedOnOpenInclusive = inclusive;
            }
            else
            {
                _model.BasedOnClose = schedProp;
                _model.BasedOnCloseInclusive = inclusive;
            }
        }
    }

    public KsqlQueryable<T1> WhenEmpty(Expression<Func<T1, T1, T1>> filler)
    {
        _model.WhenEmptyFiller = filler;
        return this;
    }

    public KsqlQueryable2<T1, T2> Join<T2>(Expression<Func<T1, T2, bool>> condition)
    {
        if (_stage != QueryBuildStage.From)
            throw new InvalidOperationException("Join() must be called immediately after From().");

        var newModel = new KsqlQueryModel
        {
            SourceTypes = new[] { typeof(T1), typeof(T2) },
            JoinCondition = condition,
            WhereCondition = _model.WhereCondition,
            SelectProjection = _model.SelectProjection
        };
        return new KsqlQueryable2<T1, T2>(newModel);
    }

    public KsqlQueryModel Build() => _model;

}
