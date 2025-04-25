from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Callable, Union, cast

from jmespath import compile as jmespath_compile
from jmespath.exceptions import JMESPathError

if TYPE_CHECKING:
    from .._queues import Message


MatcherCallable = Callable[[dict[str, Any]], bool]


class HandlerMatcher:
    matchers: list[MatcherCallable]
    operator: Callable[[Iterable[bool]], bool]

    def __init__(
        self,
        *matchers: Union[str, MatcherCallable],
        operator: Callable[[Iterable[bool]], bool] = all,
    ):
        if not matchers:
            raise ValueError("At least one matcher must be provided")

        self.matchers = [self._prepare_matchers(matcher) for matcher in matchers]
        self.operator = operator

    def __call__(self, message: "Message") -> bool:
        message_dict = message.model_dump()

        return self.operator(
            self._match(matcher, message_dict) for matcher in self.matchers
        )

    def _prepare_matchers(
        self,
        matcher: Union[str, MatcherCallable],
    ) -> MatcherCallable:
        if callable(matcher):
            return matcher

        try:
            return cast(MatcherCallable, jmespath_compile(matcher).search)
        except JMESPathError:
            raise ValueError(f"Invalid JMESPath expression: {matcher}") from None

    def _match(self, matcher: MatcherCallable, message_dict: dict[str, Any]) -> bool:
        try:
            return bool(matcher(message_dict))
        except JMESPathError:
            return False
