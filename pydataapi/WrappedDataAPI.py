import boto3
from sqlalchemy.orm import Query
from sqlalchemy import func
from pydataapi import Result, DataAPI
from typing import (
    Any,
    Dict,
    Optional,
    Type,
    Union,
)
from sqlalchemy.sql import Insert, Delete, Select, Update
from WrappedResult import WrappedResult


class WrappedDataAPI(DataAPI):

    def __init__(self,
                 *,
                 secret_arn: str,
                 resource_arn: Optional[str] = None,
                 resource_name: Optional[str] = None,
                 database: Optional[str] = None,
                 transaction_id: Optional[str] = None,
                 client: Optional[boto3.session.Session.client] = None,
                 rollback_exception: Optional[Type[Exception]] = None,
                 rds_client: Optional[boto3.session.Session.client] = None,):

        super().__init__(secret_arn=secret_arn, resource_arn=resource_arn,
                         resource_name=resource_name, database=database,
                         transaction_id=transaction_id, client=client,
                         rollback_exception=rollback_exception,
                         rds_client=rds_client)

    def _get_start_end(self, end, start=0, limit=999):
        while True:
            if start + limit >= end:
                yield (start, end)
                break
            tmp_end = start + limit
            yield (start, tmp_end)
            start = tmp_end
            start += 1

    def execute(self,
                query: Union[Query, Insert, Update, Delete, Select, str],
                parameters: Optional[Dict[str, Any]] = None,
                transaction_id: Optional[str] = None,
                continue_after_timeout: bool = True,
                database: Optional[str] = None,
                ) -> WrappedResult:
        if isinstance(query, (Query, Select)):
            qCount: Query = query.statement.with_only_columns([func.count()])\
                            .order_by(None)
            result:  Result = super().execute(qCount)
            query_total: int = result.scalar()
            final_result = list()
            for start, end in self._get_start_end(query_total):
                sliceQuery: Query = query.slice(start, end)
                result: Result = super().execute(sliceQuery, parameters,
                                                 transaction_id,
                                                 continue_after_timeout,
                                                 database)
                final_result.append(result)
            wResult: WrappedResult = WrappedResult(final_result)
        else:
            result: Result = super().execute(query, parameters, transaction_id,
                                             continue_after_timeout, database)
            wResult: WrappedResult = WrappedResult([result])
        return wResult
