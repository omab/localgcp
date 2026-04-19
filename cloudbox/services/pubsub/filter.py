"""Pub/Sub message filter evaluator.

Supports the subset of the GCP CEL-based filter syntax used in practice:

    attributes.KEY = "VALUE"
    hasPrefix(attributes.KEY, "PREFIX")
    NOT <expr>
    <expr> AND <expr>
    <expr> OR <expr>
    (<expr>)

Evaluation is case-sensitive and short-circuits AND/OR.
An empty filter string matches all messages.
"""

from __future__ import annotations

import re
import shlex


def _tokenize(expr: str) -> list[str]:
    """Return a flat token list from the filter expression.

    Args:
        expr (str): Raw filter expression string to tokenize.

    Returns:
        list[str]: Ordered list of tokens parsed from the expression.
    """
    # Pad parentheses and commas so shlex splits them as separate tokens
    expr = re.sub(r"([(),])", r" \1 ", expr)
    tokens = shlex.split(expr)
    return tokens


class _Parser:
    """Recursive-descent parser for the filter grammar."""

    def __init__(self, tokens: list[str]) -> None:
        """Initialize the parser with a token list.

        Args:
            tokens (list[str]): Ordered list of tokens produced by _tokenize.
        """
        self._tokens = tokens
        self._pos = 0

    def _peek(self) -> str | None:
        """Return the current token without consuming it.

        Returns:
            str | None: Current token, or None if the token list is exhausted.
        """
        if self._pos < len(self._tokens):
            return self._tokens[self._pos]
        return None

    def _consume(self) -> str:
        """Consume and return the current token.

        Returns:
            str: The token at the current position.
        """
        tok = self._tokens[self._pos]
        self._pos += 1
        return tok

    def _expect(self, val: str) -> None:
        """Consume the current token and raise ValueError if it does not match val.

        Args:
            val (str): Expected token value.

        Raises:
            ValueError: If the current token does not equal val.
        """
        tok = self._consume()
        if tok != val:
            raise ValueError(f"Expected {val!r}, got {tok!r}")

    def parse(self):
        """Parse the token list into an AST and verify all tokens are consumed.

        Returns:
            tuple: AST node representing the full filter expression.

        Raises:
            ValueError: If tokens remain after the expression is fully parsed.
        """
        node = self._parse_or()
        if self._pos != len(self._tokens):
            raise ValueError(f"Unexpected token: {self._tokens[self._pos]!r}")
        return node

    def _parse_or(self):
        """Parse an OR expression.

        Returns:
            tuple: AST node for the OR expression or its left-hand sub-expression.
        """
        left = self._parse_and()
        while self._peek() == "OR":
            self._consume()
            right = self._parse_and()
            left = ("OR", left, right)
        return left

    def _parse_and(self):
        """Parse an AND expression.

        Returns:
            tuple: AST node for the AND expression or its left-hand sub-expression.
        """
        left = self._parse_not()
        while self._peek() == "AND":
            self._consume()
            right = self._parse_not()
            left = ("AND", left, right)
        return left

    def _parse_not(self):
        """Parse a NOT expression or delegate to atom parsing.

        Returns:
            tuple: AST node for the NOT expression or the inner atom.
        """
        if self._peek() == "NOT":
            self._consume()
            return ("NOT", self._parse_atom())
        return self._parse_atom()

    def _parse_atom(self):
        """Parse an atomic expression: parenthesized group, hasPrefix call, or equality check.

        Returns:
            tuple: AST node for the atom.

        Raises:
            ValueError: If the current token is not a recognized atom start.
        """
        tok = self._peek()
        if tok == "(":
            self._consume()
            node = self._parse_or()
            self._expect(")")
            return node
        if tok == "hasPrefix":
            return self._parse_has_prefix()
        # attributes.KEY = "VALUE"
        if tok and tok.startswith("attributes."):
            key = self._consume()[len("attributes.") :]
            op = self._consume()
            if op != "=":
                raise ValueError(f"Unsupported operator: {op!r}")
            value = self._consume()
            return ("EQ", key, value)
        raise ValueError(f"Unexpected token in filter: {tok!r}")

    def _parse_has_prefix(self):
        """Parse a hasPrefix(attributes.KEY, PREFIX) call.

        Returns:
            tuple: AST node of the form ('HAS_PREFIX', key, prefix).

        Raises:
            ValueError: If the first argument is not an attributes.KEY reference.
        """
        self._consume()  # 'hasPrefix'
        self._expect("(")
        attr = self._consume()
        if not attr.startswith("attributes."):
            raise ValueError(f"hasPrefix first arg must be attributes.KEY, got {attr!r}")
        key = attr[len("attributes.") :]
        self._expect(",")
        prefix = self._consume()
        self._expect(")")
        return ("HAS_PREFIX", key, prefix)


def _eval(node, attributes: dict[str, str]) -> bool:
    """Evaluate an AST node against a message's attributes dict.

    Args:
        node (tuple): AST node produced by _Parser.parse().
        attributes (dict[str, str]): Message attributes to evaluate against.

    Returns:
        bool: True if the node evaluates to true for the given attributes.

    Raises:
        ValueError: If the AST contains an unknown node kind.
    """
    kind = node[0]
    if kind == "EQ":
        _, key, value = node
        return attributes.get(key) == value
    if kind == "HAS_PREFIX":
        _, key, prefix = node
        return attributes.get(key, "").startswith(prefix)
    if kind == "NOT":
        return not _eval(node[1], attributes)
    if kind == "AND":
        return _eval(node[1], attributes) and _eval(node[2], attributes)
    if kind == "OR":
        return _eval(node[1], attributes) or _eval(node[2], attributes)
    raise ValueError(f"Unknown AST node: {kind!r}")


def matches(filter_expr: str, message: dict) -> bool:
    """Return True if the message matches the filter expression.

    An empty filter matches everything. Parse or evaluation errors are treated
    as a match (fail-open) so messages are never silently dropped.

    Args:
        filter_expr (str): CEL-based filter expression string, or empty string to match all.
        message (dict): PubsubMessage dict containing an 'attributes' key.

    Returns:
        bool: True if the message matches the filter, False otherwise.
    """
    if not filter_expr:
        return True
    attributes = message.get("attributes") or {}
    try:
        tokens = _tokenize(filter_expr)
        ast = _Parser(tokens).parse()
        return _eval(ast, attributes)
    except Exception:
        return True  # fail-open on malformed filter
