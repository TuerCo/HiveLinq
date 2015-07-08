using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.ResultOperators;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace HiveLinq.HiveQueryGeneration
{
    public class HiveqlGeneratorQueryModelVisitor : QueryModelVisitorBase
    {
        public static CommandData GenerateHiveqlQuery(QueryModel queryModel)
        {
            var visitor = new HiveqlGeneratorQueryModelVisitor();
            visitor.VisitQueryModel(queryModel);
            return visitor.GetHiveqlCommand();
        }

        private readonly QueryPartsAggregator _queryParts = new QueryPartsAggregator();

        private readonly ParameterAggregator _parameterAggregator = new ParameterAggregator();

        public CommandData GetHiveqlCommand()
        {
            return new CommandData(_queryParts.BuildHQLString(), _parameterAggregator.GetParameters());
        }

        public override void VisitQueryModel(QueryModel queryModel)
        {
            queryModel.SelectClause.Accept(this, queryModel);
            queryModel.MainFromClause.Accept(this, queryModel);
            VisitBodyClauses(queryModel.BodyClauses, queryModel);
            VisitResultOperators(queryModel.ResultOperators, queryModel);
        }

        public override void VisitResultOperator(ResultOperatorBase resultOperator, QueryModel queryModel, int index)
        {
            if (resultOperator is CountResultOperator)
            {
                _queryParts.SelectPart = string.Format("cast(count(1) as int)", _queryParts.SelectPart);
            }
            else if (resultOperator is SumResultOperator)
            {
                _queryParts.SelectPart = string.Format("sum({0})", _queryParts.SelectPart);
            }
            else if (resultOperator is MinResultOperator)
            {
                _queryParts.SelectPart = string.Format("min({0})", _queryParts.SelectPart);
            }
            else if (resultOperator is MaxResultOperator)
            {
                _queryParts.SelectPart = string.Format("max({0})", _queryParts.SelectPart);
            }
            else if (resultOperator is GroupResultOperator)
            {
                var groupResultOperator = resultOperator as GroupResultOperator;
                var expression = GetHiveqlExpression(groupResultOperator.KeySelector);
                _queryParts.AddGroupByPart(new List<string> { expression });
            }
            else
                throw new NotSupportedException("Only Count, Sum, Min, Max result operator is showcased.");

            base.VisitResultOperator(resultOperator, queryModel, index);
        }

        public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
        {
            _queryParts.AddFromPart(fromClause);

            base.VisitMainFromClause(fromClause, queryModel);
        }

        public override void VisitSelectClause(SelectClause selectClause, QueryModel queryModel)
        {
            _queryParts.SelectPart = GetHiveqlExpression(selectClause.Selector);

            base.VisitSelectClause(selectClause, queryModel);
        }

        public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
        {
            _queryParts.AddWherePart(GetHiveqlExpression(whereClause.Predicate));

            base.VisitWhereClause(whereClause, queryModel, index);
        }

        public override void VisitOrderByClause(OrderByClause orderByClause, QueryModel queryModel, int index)
        {
            _queryParts.AddOrderByPart(orderByClause.Orderings.Select(o => GetHiveqlExpression(o.Expression)));

            base.VisitOrderByClause(orderByClause, queryModel, index);
        }

        public override void VisitJoinClause(JoinClause joinClause, QueryModel queryModel, int index)
        {
            // HQL joins work differently, need to simulate using a cross join with a where condition

            _queryParts.AddFromPart(joinClause);
            _queryParts.AddWherePart(
                "({0} = {1})",
                GetHiveqlExpression(joinClause.OuterKeySelector),
                GetHiveqlExpression(joinClause.InnerKeySelector));

            base.VisitJoinClause(joinClause, queryModel, index);
        }

        public override void VisitAdditionalFromClause(AdditionalFromClause fromClause, QueryModel queryModel, int index)
        {
            _queryParts.AddFromPart(fromClause);

            base.VisitAdditionalFromClause(fromClause, queryModel, index);
        }

        public override void VisitGroupJoinClause(GroupJoinClause groupJoinClause, QueryModel queryModel, int index)
        {
            throw new NotSupportedException("Adding a join ... into ... implementation to the query provider is left to the reader for extra points.");
        }

        private string GetHiveqlExpression(Expression expression)
        {
            return HiveqlGeneratorExpressionTreeVisitor.GetHiveqlExpression(expression, _parameterAggregator);
        }
    }
}