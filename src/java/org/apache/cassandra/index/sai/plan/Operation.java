/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.plan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;

import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.schema.ColumnMetadata;

public class Operation
{
    public enum BooleanOperator
    {
        AND((a, b) -> a & b),
        OR((a, b) -> a | b);

        private final BiFunction<Boolean, Boolean, Boolean> func;

        BooleanOperator(BiFunction<Boolean, Boolean, Boolean> func)
        {
            this.func = func;
        }

        public boolean apply(boolean a, boolean b)
        {
            return func.apply(a, b);
        }
    }

    public static class Expressions
    {
        final ListMultimap<ColumnMetadata, Expression> expressions;
        final Set<ColumnMetadata> unindexedColumns;

        Expressions(ListMultimap<ColumnMetadata, Expression> expressions, Set<ColumnMetadata> unindexedColumns)
        {
            this.expressions = expressions;
            this.unindexedColumns = unindexedColumns;
        }

        Set<ColumnMetadata> columns()
        {
            return expressions.keySet();
        }

        Collection<Expression> all()
        {
            return expressions.values();
        }

        List<Expression> expressionsFor(ColumnMetadata column)
        {
            return expressions.get(column);
        }

        boolean isEmpty()
        {
            return expressions.isEmpty();
        }

        int size()
        {
            return expressions.size();
        }

        boolean isUnindexed(ColumnMetadata column)
        {
            return unindexedColumns.contains(column);
        }

        boolean hasMultipleUnindexedColumns()
        {
            return unindexedColumns.size() > 1;
        }
    }

    @VisibleForTesting
    protected static Expressions buildIndexExpressions(QueryController queryController, List<RowFilter.Expression> expressions)
    {
        ListMultimap<ColumnMetadata, Expression> analyzed = ArrayListMultimap.create();
        Set<ColumnMetadata> unindexedColumns = Collections.emptySet();

        // sort all the expressions in the operation by name and priority of the logical operator
        // this gives us an efficient way to handle inequality and combining into ranges without extra processing
        // and converting expressions from one type to another.
        expressions.sort((a, b) -> {
            int cmp = a.column().compareTo(b.column());
            return cmp == 0 ? -Integer.compare(getPriority(a.operator()), getPriority(b.operator())) : cmp;
        });

        for (final RowFilter.Expression expression : expressions)
        {
            if (Expression.supportsOperator(expression.operator()))
            {
                StorageAttachedIndex index = queryController.indexFor(expression);
                List<Expression> perColumn = analyzed.get(expression.column());

                if (index == null)
                {
                    buildUnindexedExpression(queryController, expression, perColumn);

                    if (!expression.column().isPrimaryKeyColumn())
                    {
                        if (unindexedColumns.isEmpty())
                            unindexedColumns = new HashSet<>(3);

                        unindexedColumns.add(expression.column());
                    }
                }
                else
                {
                    buildIndexedExpression(index, expression, perColumn);
                }
            }
        }

        return new Expressions(analyzed, unindexedColumns);
    }

    private static void buildUnindexedExpression(QueryController queryController,
                                                 RowFilter.Expression expression,
                                                 List<Expression> perColumn)
    {
        IndexTermType indexTermType = IndexTermType.create(expression.column(),
                                                           queryController.metadata().partitionKeyColumns(),
                                                           determineIndexTargetType(expression));
        if (indexTermType.isMultiExpression(expression))
        {
            perColumn.add(Expression.create(indexTermType).add(expression.operator(), expression.getIndexValue().duplicate()));
        }
        else
        {
            Expression range;
            if (perColumn.size() == 0)
            {
                range = Expression.create(indexTermType);
                perColumn.add(range);
            }
            else
            {
                range = Iterables.getLast(perColumn);
            }
            range.add(expression.operator(), expression.getIndexValue().duplicate());
        }
    }

    private static void buildIndexedExpression(StorageAttachedIndex index, RowFilter.Expression expression, List<Expression> perColumn)
    {
        if (index.hasAnalyzer())
        {
            AbstractAnalyzer analyzer = index.analyzer();
            try
            {
                analyzer.reset(expression.getIndexValue().duplicate());

                if (index.termType().isMultiExpression(expression))
                {
                    while (analyzer.hasNext())
                    {
                        final ByteBuffer token = analyzer.next();
                        perColumn.add(Expression.create(index).add(expression.operator(), token.duplicate()));
                    }
                }
                else
                // "range" or not-equals operator, combines both bounds together into the single expression,
                // if operation of the group is AND, otherwise we are forced to create separate expressions,
                // not-equals is combined with the range iff operator is AND.
                {
                    Expression range;
                    if (perColumn.size() == 0)
                    {
                        range = Expression.create(index);
                        perColumn.add(range);
                    }
                    else
                    {
                        range = Iterables.getLast(perColumn);
                    }

                    if (index.termType().isLiteral())
                    {
                        while (analyzer.hasNext())
                        {
                            ByteBuffer term = analyzer.next();
                            range.add(expression.operator(), term.duplicate());
                        }
                    }
                    else
                    {
                        range.add(expression.operator(), expression.getIndexValue().duplicate());
                    }
                }
            }
            finally
            {
                analyzer.end();
            }
        }
        else
        {
            if (index.termType().isMultiExpression(expression))
            {
                perColumn.add(Expression.create(index).add(expression.operator(), expression.getIndexValue().duplicate()));
            }
            else
            {
                Expression range;
                if (perColumn.size() == 0)
                {
                    range = Expression.create(index);
                    perColumn.add(range);
                }
                else
                {
                    range = Iterables.getLast(perColumn);
                }
                range.add(expression.operator(), expression.getIndexValue().duplicate());
            }
        }
    }

    /**
     * Determines the {@link IndexTarget.Type} for the expression. In this case we are only interested in map types and
     * the operator being used in the expression.
     */
    private static IndexTarget.Type determineIndexTargetType(RowFilter.Expression expression)
    {
        AbstractType<?> type  = expression.column().type;
        IndexTarget.Type indexTargetType = IndexTarget.Type.SIMPLE;
        if (type.isCollection() && type.isMultiCell())
        {
            CollectionType<?> collection = ((CollectionType<?>) type);
            if (collection.kind == CollectionType.Kind.MAP)
            {
                switch (expression.operator())
                {
                    case EQ:
                        indexTargetType = IndexTarget.Type.KEYS_AND_VALUES;
                        break;
                    case CONTAINS:
                        indexTargetType = IndexTarget.Type.VALUES;
                        break;
                    case CONTAINS_KEY:
                        indexTargetType = IndexTarget.Type.KEYS;
                        break;
                    default:
                        throw new InvalidRequestException("Invalid operator");
                }
            }
        }
        return indexTargetType;
    }

    private static int getPriority(Operator op)
    {
        switch (op)
        {
            case EQ:
            case CONTAINS:
            case CONTAINS_KEY:
                return 5;

            case GTE:
            case GT:
                return 3;

            case LTE:
            case LT:
                return 2;

            default:
                return 0;
        }
    }

    /**
     * Converts expressions into filter tree for query.
     *
     * @return a KeyRangeIterator over the index query results
     */
    static KeyRangeIterator buildIterator(QueryController controller)
    {
        List<RowFilter.Expression> orderings = controller.indexFilter().getExpressions()
                                                         .stream().filter(e -> e.operator() == Operator.ANN).collect(Collectors.toList());
        assert orderings.size() <= 1;
        if (controller.indexFilter().getExpressions().size() == 1 && orderings.size() == 1)
            // If we only have one expression, we just use the ANN index to order and limit.
            return controller.getTopKRows(orderings.get(0));
        KeyRangeIterator iterator = Node.buildTree(controller.indexFilter()).analyzeTree(controller).rangeIterator(controller);
        if (orderings.isEmpty())
            return iterator;
        return controller.getTopKRows(iterator, orderings.get(0));
    }

    /**
     * Converts expressions into filter tree (which is currently just a single AND).
     * <p>
     * Filter tree allows us to do a couple of important optimizations
     * namely, group flattening for AND operations (query rewrite), expression bounds checks,
     * "satisfies by" checks for resulting rows with an early exit.
     *
     * @return root of the filter tree.
     */
    static FilterTree buildFilter(QueryController controller, boolean strict)
    {
        return Node.buildTree(controller.indexFilter()).buildFilter(controller, strict);
    }

    static abstract class Node
    {
        Expressions expressions;

        boolean canFilter()
        {
            return (expressions != null && !expressions.isEmpty()) || !children().isEmpty();
        }

        List<Node> children()
        {
            return Collections.emptyList();
        }

        void add(Node child)
        {
            throw new UnsupportedOperationException();
        }

        RowFilter.Expression expression()
        {
            throw new UnsupportedOperationException();
        }

        abstract void analyze(List<RowFilter.Expression> expressionList, QueryController controller);

        abstract FilterTree filterTree(boolean strict, QueryContext context);

        abstract KeyRangeIterator rangeIterator(QueryController controller);

        static Node buildTree(RowFilter filterOperation)
        {
            OperatorNode node = new AndNode();
            for (RowFilter.Expression expression : filterOperation.getExpressions())
                node.add(buildExpression(expression));
            return node;
        }

        static Node buildExpression(RowFilter.Expression expression)
        {
            return new ExpressionNode(expression);
        }

        Node analyzeTree(QueryController controller)
        {
            List<RowFilter.Expression> expressionList = new ArrayList<>();
            doTreeAnalysis(this, expressionList, controller);
            if (!expressionList.isEmpty())
                this.analyze(expressionList, controller);
            return this;
        }

        void doTreeAnalysis(Node node, List<RowFilter.Expression> expressions, QueryController controller)
        {
            if (node.children().isEmpty())
                expressions.add(node.expression());
            else
            {
                List<RowFilter.Expression> expressionList = new ArrayList<>();
                for (Node child : node.children())
                    doTreeAnalysis(child, expressionList, controller);
                node.analyze(expressionList, controller);
            }
        }

        FilterTree buildFilter(QueryController controller, boolean isStrict)
        {
            analyzeTree(controller);
            FilterTree tree = filterTree(isStrict, controller.queryContext);
            for (Node child : children())
                if (child.canFilter())
                    tree.addChild(child.buildFilter(controller, isStrict));
            return tree;
        }
    }

    static abstract class OperatorNode extends Node
    {
        final List<Node> children = new ArrayList<>();

        @Override
        public List<Node> children()
        {
            return children;
        }

        @Override
        public void add(Node child)
        {
            children.add(child);
        }
    }

    static class AndNode extends OperatorNode
    {
        @Override
        public void analyze(List<RowFilter.Expression> expressionList, QueryController controller)
        {
            expressions = buildIndexExpressions(controller, expressionList);
        }

        @Override
        FilterTree filterTree(boolean isStrict, QueryContext context)
        {
            return new FilterTree(BooleanOperator.AND, expressions, isStrict, context);
        }

        @Override
        KeyRangeIterator rangeIterator(QueryController controller)
        {
            KeyRangeIterator.Builder builder = controller.getIndexQueryResults(expressions.all());
            for (Node child : children)
            {
                boolean canFilter = child.canFilter();
                if (canFilter)
                    builder.add(child.rangeIterator(controller));
            }
            return builder.build();
        }
    }

    static class ExpressionNode extends Node
    {
        final RowFilter.Expression expression;

        @Override
        public void analyze(List<RowFilter.Expression> expressionList, QueryController controller)
        {
            expressions = buildIndexExpressions(controller, expressionList);
            assert expressions.size() == 1 : "Expression nodes should only have a single expression!";
        }

        @Override
        FilterTree filterTree(boolean isStrict, QueryContext context)
        {
            // There should only be one expression, so AND/OR would both work here. 
            return new FilterTree(BooleanOperator.AND, expressions, isStrict, context);
        }

        public ExpressionNode(RowFilter.Expression expression)
        {
            this.expression = expression;
        }

        @Override
        public RowFilter.Expression expression()
        {
            return expression;
        }

        @Override
        KeyRangeIterator rangeIterator(QueryController controller)
        {
            assert canFilter() : "Cannot process query with no expressions";

            return controller.getIndexQueryResults(expressions.all()).build();
        }
    }
}
