from pydataapi import Result
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
)


class WrappedResult(Result):
    def __init__(self, result_list: Sequence[Result], isSelect=False):
        self._number_of_records_updated = 0
        self._column_metadata: List[Dict[str, Any]] = list()
        self._rows = list()
        self._index: int = -1
        self._headers: Optional[List[str]] = None
        self.generatedFields: List[Any] = list()
        self.last_generatedFields: Any = 0
        for result in result_list:
            self._rows += result._rows
            self._column_metadata += result._column_metadata
            self._number_of_records_updated += result.number_of_records_updated
            if "generatedFields" in result._response and len(
                                    result._response["generatedFields"]) > 0:
                self.generatedFields.append(
                    [list(f.values())[0] for f in
                        result._response["generatedFields"]][0]
                )
                self.last_generatedFields = self.generatedFields[-1]
        if len(result_list) > 0:
            self._headers = result_list[0].headers
            self._response = result_list[0].headers

    @property
    def number_of_records_updated(self) -> int:
        return self._number_of_records_updated

    @property
    def headers(self) -> List[str]:
        return self._headers
