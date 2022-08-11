from pglast import parser
from munch import munchify
import json
import sqlparse


def target_is_aggregate(target):
    try:
        for funcname in target.ResTarget.val.FuncCall.funcname:
            if funcname.String.str in ("sum", "avg", "min", "max", "count"):
                return True
    except AttributeError:
        pass


class Query(object):
    def __init__(self, query_text: str):
        self.query_text = query_text
        self.parsed_query = munchify(x=json.loads(parser.parse_sql_json(query=self.query_text))).stmts[0]
        self._validate_query()
        self.select_stmt = self.parsed_query.stmt.SelectStmt

        self.summary_query = self.__get_summary_query()

    def _validate_query(self):
        if not hasattr(self.parsed_query.stmt, "SelectStmt"):
            raise RuntimeError("The query is NOT a SELECT statement - it is not supported.")

    @property
    def has_aggregates(self):
        for target in self.select_stmt.targetList:
            if target_is_aggregate(target):
                return True
        return False

    def __get_summary_query(self):
        if self.has_aggregates:
            group_by_clause = ""
            aggregate_clause = ""
            for target in self.select_stmt.targetList:
                if not target_is_aggregate(target):
                    group_by_clause += f", {target.ResTarget.val.ColumnRef.fields[0].String.str}"
                else:
                    raw_aggregate_function = target.ResTarget.val.FuncCall.funcname[0].String.str.upper()
                    if raw_aggregate_function in ("SUM", "COUNT"):
                        summary_aggregate_function = "SUM"
                    else:
                        summary_aggregate_function = raw_aggregate_function

                    if hasattr(target.ResTarget, "name"):
                        column_name = target.ResTarget.name
                    elif getattr(target.ResTarget.val.FuncCall, "agg_star", False):
                        column_name = f'"{target.ResTarget.val.FuncCall.funcname[0].String.str}_star()"'
                    else:
                        column_name = f'"{target.ResTarget.val.FuncCall.funcname[0].String.str}({target.ResTarget.val.FuncCall.args[0].ColumnRef.fields[0].String.str})"'

                    aggregate_clause += f", {summary_aggregate_function} ({column_name}) AS {column_name}"

            select_column_sql = (group_by_clause + aggregate_clause).strip(', ')
            if group_by_clause:
                group_by_clause = f" GROUP BY {group_by_clause.lstrip(', ')}"

            return_sql = f"SELECT {select_column_sql} FROM combined_result {group_by_clause}"
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
#        l_returnflag,
#        l_linestatus;""")
#
# print(x)
