import json

import sqlparse
from munch import munchify
from pglast import parser


def target_is_aggregate(target):
    try:
        for funcname in target.ResTarget.val.FuncCall.funcname:
            if funcname.String.sval in ("sum", "avg", "min", "max", "count"):
                return True
    except AttributeError:
        pass


class Query(object):
    def __init__(self, query_text: str):
        self.query_text = query_text
        self.parsed_query = munchify(
            x=json.loads(parser.parse_sql_json(query=self.query_text))).stmts[0]
        self._validate_query()
        self.select_stmt = self.parsed_query.stmt.SelectStmt

        self.summary_query = self.__get_summary_query()

    def _validate_query(self):
        if not hasattr(self.parsed_query.stmt, "SelectStmt"):
            raise RuntimeError(
                "The query is NOT a SELECT statement - it is not supported.")

    @property
    def has_aggregates(self):
        for target in self.select_stmt.targetList:
            if target_is_aggregate(target):
                return True
        return False

    def __get_summary_query(self):
        def __get_select_and_group_by_sql():
            select_column_sql = ""
            group_by_clause = ""
            for target in self.select_stmt.targetList:
                if not target_is_aggregate(target):
                    column_name = target.ResTarget.val.ColumnRef.fields[-1].String.sval
                    group_by_clause += f", {column_name}"
                    select_column_sql += f", {column_name}"
                else:
                    raw_aggregate_function = \
                        target.ResTarget.val.FuncCall.funcname[
                            0].String.sval.upper()
                    if raw_aggregate_function in ("SUM", "COUNT"):
                        summary_aggregate_function = "SUM"
                    else:
                        summary_aggregate_function = raw_aggregate_function

                    if hasattr(target.ResTarget, "name"):
                        column_name = target.ResTarget.name
                    elif getattr(target.ResTarget.val.FuncCall, "agg_star",
                                 False):
                        column_name = f'"{target.ResTarget.val.FuncCall.funcname[0].String.sval}_star()"'
                    else:
                        column_name = (f'"{target.ResTarget.val.FuncCall.funcname[0].String.sval}('
                                       f'{"DISTINCT " if getattr(target.ResTarget.val.FuncCall, "agg_distinct", False) else ""}'
                                       f'{target.ResTarget.val.FuncCall.args[0].ColumnRef.fields[-1].String.sval})"'
                                       )

                    aggregate_clause = f", {summary_aggregate_function} ({column_name}) AS {column_name}"
                    select_column_sql += aggregate_clause

            select_column_sql = select_column_sql.strip(', ')
            if group_by_clause:
                group_by_clause = f" GROUP BY {group_by_clause.lstrip(', ')}"

            return select_column_sql, group_by_clause

        def __get_order_by_clause():
            sort_columns = getattr(self.select_stmt, "sortClause", None)
            order_by_clause = ""
            if sort_columns:
                order_by_clause = "\nORDER BY "
                comma = ""
                for sort_column in sort_columns:
                    if hasattr(sort_column.SortBy.node, "ColumnRef"):
                        column_name = sort_column.SortBy.node.ColumnRef.fields[-1].String.sval
                    elif hasattr(sort_column.SortBy.node, "A_Const"):
                        column_name = sort_column.SortBy.node.A_Const.ival.ival
                    sort_direction = "ASC" if sort_column.SortBy.sortby_dir == "SORTBY_DEFAULT" else "DESC"
                    sort_nulls = "NULLS FIRST" if sort_column.SortBy.sortby_nulls == "SORTBY_NULLS_DEFAULT" else "NULLS LAST"
                    order_by_clause += f"{comma}{column_name} {sort_direction} {sort_nulls}"
                    comma = ",\n"

            return order_by_clause

        def __get_limit_clause():
            limit_option = getattr(self.select_stmt, "limitCount", None)
            limit_clause = ""
            if limit_option:
                limit_clause = f"\nLIMIT {limit_option.A_Const.ival.ival}"

            return limit_clause

        if self.has_aggregates:
            select_column_sql, group_by_clause = __get_select_and_group_by_sql()
            order_by_clause = __get_order_by_clause()
            limit_clause = __get_limit_clause()

            return_sql = f"SELECT {select_column_sql} FROM combined_result {group_by_clause}{order_by_clause}{limit_clause}"
            return sqlparse.format(sql=return_sql,
                                   reindent=True,
                                   keyword_case='upper',
                                   identifier_case='lower',
                                   use_space_around_operators=True,
                                   comma_first=True
                                   )

# x = Query(query_text="""
# select
#        l_returnflag,
#        l_linestatus,
#        sum(l_quantity) as sum_qty,
#        sum(l_extendedprice) as sum_base_price,
#        sum(l_extendedprice * (1-l_discount)) as sum_disc_price,
#        sum(l_extendedprice * (1-l_discount) * (1+l_tax)) as sum_charge,
#        avg(l_quantity) as avg_qty,
#        avg(l_extendedprice) as avg_price,
#        avg(l_discount) as avg_disc,
#        count(*) as count_order
#  from
#        lineitem
#  where
#        l_shipdate <= DATE '1998-12-01' - 90
#  group by
#        l_returnflag,
#        l_linestatus
#  order by
#        1, 2
# limit 100;""")
#
# print(x)
