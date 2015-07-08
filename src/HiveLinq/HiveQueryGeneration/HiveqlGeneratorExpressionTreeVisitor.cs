using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Clauses.ExpressionTreeVisitors;
using Remotion.Linq.Parsing;
using System;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace HiveLinq.HiveQueryGeneration
{
    public class HiveqlGeneratorExpressionTreeVisitor : ThrowingExpressionTreeVisitor
    {
        public static string GetHiveqlExpression(Expression linqExpression, ParameterAggregator parameterAggregator)
        {
            var visitor = new HiveqlGeneratorExpressionTreeVisitor(parameterAggregator);
            visitor.VisitExpression(linqExpression);
            return visitor.GetHiveqlExpression();
        }

        private readonly StringBuilder _hqlExpression = new StringBuilder();
        private readonly ParameterAggregator _parameterAggregator;

        private HiveqlGeneratorExpressionTreeVisitor(ParameterAggregator parameterAggregator)
        {
            _parameterAggregator = parameterAggregator;
        }

        public string GetHiveqlExpression()
        {
            return _hqlExpression.ToString();
        }

        protected override Expression VisitNewExpression(NewExpression expression)
        {
            for (int i = 0; i < expression.Members.Count; i++)
            {
                var item = expression.Arguments[i];

                if (i > 0)
                {
                    _hqlExpression.Append(", ");
                }

                VisitExpression(item);
            }

            return expression;
        }

        protected override Expression VisitQuerySourceReferenceExpression(QuerySourceReferenceExpression expression)
        {
            _hqlExpression.Append(expression.ReferencedQuerySource.ItemName);
            return expression;
        }

        protected override Expression VisitBinaryExpression(BinaryExpression expression)
        {
            _hqlExpression.Append("(");

            VisitExpression(expression.Left);

            // In production code, handle this via lookup tables.
            switch (expression.NodeType)
            {
                case ExpressionType.Equal:
                    _hqlExpression.Append(" = ");
                    break;

                case ExpressionType.AndAlso:
                case ExpressionType.And:
                    _hqlExpression.Append(" and ");
                    break;

                case ExpressionType.OrElse:
                case ExpressionType.Or:
                    _hqlExpression.Append(" or ");
                    break;

                case ExpressionType.Add:
                    _hqlExpression.Append(" + ");
                    break;

                case ExpressionType.Subtract:
                    _hqlExpression.Append(" - ");
                    break;

                case ExpressionType.Multiply:
                    _hqlExpression.Append(" * ");
                    break;

                case ExpressionType.Divide:
                    _hqlExpression.Append(" / ");
                    break;

                case ExpressionType.GreaterThan:
                    _hqlExpression.Append(" > ");
                    break;

                case ExpressionType.GreaterThanOrEqual:
                    _hqlExpression.Append(" >= ");
                    break;

                case ExpressionType.LessThan:
                    _hqlExpression.Append(" < ");
                    break;

                case ExpressionType.LessThanOrEqual:
                    _hqlExpression.Append(" <= ");
                    break;

                default:
                    base.VisitBinaryExpression(expression);
                    break;
            }

            VisitExpression(expression.Right);
            _hqlExpression.Append(")");

            return expression;
        }

        protected override Expression VisitMemberExpression(MemberExpression expression)
        {
            VisitExpression(expression.Expression);
            _hqlExpression.AppendFormat(".{0}", expression.Member.Name);

            return expression;
        }

        protected override Expression VisitConstantExpression(ConstantExpression expression)
        {
            var namedParameter = _parameterAggregator.AddParameter(expression.Value);
            _hqlExpression.AppendFormat(":{0}", namedParameter.Name);

            return expression;
        }

        protected override Expression VisitMethodCallExpression(MethodCallExpression expression)
        {
            // In production code, handle this via method lookup tables.

            var supportedMethod = typeof(string).GetMethod("Contains");
            if (expression.Method.Equals(supportedMethod))
            {
                _hqlExpression.Append("(");
                VisitExpression(expression.Object);
                _hqlExpression.Append(" like '%'+");
                VisitExpression(expression.Arguments[0]);
                _hqlExpression.Append("+'%')");
                return expression;
            }
            else
            {
                return base.VisitMethodCallExpression(expression); // throws
            }
        }

        // Called when a LINQ expression type is not handled above.
        protected override Exception CreateUnhandledItemException<T>(T unhandledItem, string visitMethod)
        {
            string itemText = FormatUnhandledItem(unhandledItem);
            var message = string.Format("The expression '{0}' (type: {1}) is not supported by this LINQ provider.", itemText, typeof(T));
            return new NotSupportedException(message);
        }

        private string FormatUnhandledItem<T>(T unhandledItem)
        {
            var itemAsExpression = unhandledItem as Expression;
            return itemAsExpression != null ? FormattingExpressionTreeVisitor.Format(itemAsExpression) : unhandledItem.ToString();
        }
    }
}