from typing import TYPE_CHECKING, Literal

from jmespath import compile as jmespath_compile
from jmespath.exceptions import JMESPathError
from jmespath.parser import ParsedResult

if TYPE_CHECKING:
    from .._queues import Message


class HandlerMatcher:
    _matchers: list[ParsedResult]
    _operator: Literal["all", "any", "and", "or"]

    def __init__(
        self,
        *matchers: str,
        operator: Literal["all", "any", "and", "or"] = "all",
    ):
        if not matchers:
            raise ValueError("At least one matcher must be provided")

        self._matchers = []
        self._operator = operator

        for matcher in matchers:
            try:
                compiled_jmespath = jmespath_compile(matcher)
            except JMESPathError:
                raise ValueError(f"Invalid JMESPath expression: {matcher}") from None

            self._matchers.append(compiled_jmespath)

    def __call__(self, message: "Message") -> bool:
        results = []

        for matcher in self._matchers:
            try:
                results.append(bool(matcher.search(message.model_dump())))
            except JMESPathError:
                results.append(False)

        if self._operator.lower() in {"all", "and"}:
            return all(results)
        elif self._operator.lower() in {"any", "or"}:
            return any(results)

        raise ValueError("Invalid operator")
