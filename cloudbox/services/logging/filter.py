"""Cloud Logging filter language parser and evaluator.

Implements the subset of the Cloud Logging query language used in practice:

  - Logical operators: AND, OR, NOT (case-insensitive); implicit AND when no
    operator appears between two terms
  - Parentheses for grouping
  - Comparison operators: = != >= <= > <
  - Substring/contains operator: :
  - Dot-notation field paths: resource.type, resource.labels.zone,
    jsonPayload.field, labels.key, httpRequest.status, etc.
  - Bare field paths (existence / non-empty check)
  - Quoted (double or single) and unquoted values
  - Special semantics for known fields:
      severity  — compared by level order, not lexicographically
      timestamp — ISO 8601 string lexicographic comparison (valid for UTC)
      logName   — exact string match for =, substring for :

Reference: https://cloud.google.com/logging/docs/view/logging-query-language
"""

from __future__ import annotations

import re
from typing import Any

# ---------------------------------------------------------------------------
# Severity ordering
# ---------------------------------------------------------------------------

_SEVERITY_ORDER: dict[str, int] = {
    "DEFAULT": 0,
    "DEBUG": 100,
    "INFO": 200,
    "NOTICE": 300,
    "WARNING": 400,
    "ERROR": 500,
    "CRITICAL": 600,
    "ALERT": 700,
    "EMERGENCY": 800,
}

# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------

# Operators must be matched longest-first to catch != >= <= before = > <
_OP_RE = re.compile(r"!=|>=|<=|>|<|=|:")
_WS_RE = re.compile(r"\s+")
# Characters that cannot appear in an unquoted token
_UNQUOTED_STOP = set(" \t\n\r()=!<>:'\"")


def _tokenize(text: str) -> list[str]:
    """Split a filter expression into a flat list of tokens.

    Args:
        text (str): Raw filter expression.

    Returns:
        list[str]: Ordered list of string tokens (keywords, operators,
            parentheses, quoted strings, field paths, bare values).
    """
    tokens: list[str] = []
    i = 0
    n = len(text)
    while i < n:
        # Whitespace
        m = _WS_RE.match(text, i)
        if m:
            i = m.end()
            continue

        # Quoted string (double or single quotes)
        if text[i] in ('"', "'"):
            q = text[i]
            j = i + 1
            while j < n and text[j] != q:
                if text[j] == "\\":
                    j += 1  # skip escaped character
                j += 1
            tokens.append(text[i : j + 1])
            i = j + 1
            continue

        # Parentheses
        if text[i] in ("(", ")"):
            tokens.append(text[i])
            i += 1
            continue

        # Operators (longest-match first via regex alternation order)
        m = _OP_RE.match(text, i)
        if m:
            tokens.append(m.group())
            i = m.end()
            continue

        # Unquoted token — field path, keyword (AND/OR/NOT), or bare value
        j = i
        while j < n and text[j] not in _UNQUOTED_STOP:
            j += 1
        if j > i:
            tokens.append(text[i:j])
            i = j
            continue

        # Unknown character — skip
        i += 1

    return tokens


# ---------------------------------------------------------------------------
# AST node constructors (lightweight tuples)
# ---------------------------------------------------------------------------

_TRUE = ("TRUE",)
_FALSE = ("FALSE",)


def _and(left: tuple, right: tuple) -> tuple:
    return ("AND", left, right)


def _or(left: tuple, right: tuple) -> tuple:
    return ("OR", left, right)


def _not(expr: tuple) -> tuple:
    return ("NOT", expr)


def _cmp(field: str, op: str, value: str) -> tuple:
    return ("CMP", field, op, value)


def _exists(field: str) -> tuple:
    return ("EXISTS", field)


# ---------------------------------------------------------------------------
# Recursive-descent parser
# ---------------------------------------------------------------------------


class _Parser:
    """Parse a token list into an AST."""

    def __init__(self, tokens: list[str]) -> None:
        """Initialize the parser.

        Args:
            tokens (list[str]): Token list from _tokenize.
        """
        self._tokens = tokens
        self._pos = 0

    def _peek(self) -> str:
        """Return the current token without advancing."""
        return self._tokens[self._pos] if self._pos < len(self._tokens) else ""

    def _consume(self) -> str:
        """Return and advance past the current token."""
        tok = self._peek()
        self._pos += 1
        return tok

    def parse(self) -> tuple:
        """Parse the full expression and return the root AST node.

        Returns:
            tuple: Root AST node.
        """
        if not self._tokens:
            return _TRUE
        node = self._parse_or()
        return node

    def _parse_or(self) -> tuple:
        left = self._parse_and()
        while self._peek().upper() == "OR":
            self._consume()
            right = self._parse_and()
            left = _or(left, right)
        return left

    def _parse_and(self) -> tuple:
        left = self._parse_not()
        while True:
            tok = self._peek()
            if not tok or tok == ")" or tok.upper() == "OR":
                break
            if tok.upper() == "AND":
                self._consume()  # explicit AND keyword
            # implicit AND — next token starts a new term (field, NOT, or '(')
            right = self._parse_not()
            left = _and(left, right)
        return left

    def _parse_not(self) -> tuple:
        if self._peek().upper() == "NOT":
            self._consume()
            return _not(self._parse_not())
        return self._parse_atom()

    def _parse_atom(self) -> tuple:
        tok = self._peek()
        if not tok:
            return _TRUE

        if tok == "(":
            self._consume()
            node = self._parse_or()
            if self._peek() == ")":
                self._consume()
            return node

        # First token of a comparison or bare field
        field = self._consume()
        if not field:
            return _TRUE

        # Check for operator
        op = self._peek()
        if op in ("=", "!=", ">=", "<=", ">", "<", ":"):
            self._consume()
            raw_val = self._consume()
            value = _unquote(raw_val)
            return _cmp(field, op, value)

        # Bare token — if it is AND/OR/NOT put it back, otherwise field existence
        upper = field.upper()
        if upper in ("AND", "OR", "NOT"):
            # Misplaced keyword — treat as existence check on its lowercase form
            return _exists(field.lower())

        return _exists(field)


def _unquote(s: str) -> str:
    """Strip surrounding quotes from a token if present.

    Args:
        s (str): Raw token string, possibly quoted.

    Returns:
        str: Unquoted string value.
    """
    if len(s) >= 2 and s[0] in ('"', "'") and s[-1] == s[0]:
        return s[1:-1]
    return s


# ---------------------------------------------------------------------------
# Field extraction
# ---------------------------------------------------------------------------


def _get_field(entry: dict, field_path: str) -> Any:
    """Extract a value from a nested dict using dot-notation.

    Args:
        entry (dict): Log entry dict.
        field_path (str): Dot-separated path such as 'resource.type' or
            'jsonPayload.field'.

    Returns:
        Any: The value at that path, or None if any segment is missing.
    """
    parts = field_path.split(".")
    obj: Any = entry
    for part in parts:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(part)
    return obj


# ---------------------------------------------------------------------------
# Comparator
# ---------------------------------------------------------------------------


def _compare_values(field_path: str, op: str, value: str, entry: dict) -> bool:
    """Evaluate a single comparison node against a log entry.

    Args:
        field_path (str): Dot-notation field path.
        op (str): Comparison operator.
        value (str): Target value from the filter expression.
        entry (dict): Log entry dict.

    Returns:
        bool: True if the comparison holds.
    """
    actual = _get_field(entry, field_path)

    # ----- severity (level-order comparison) -----
    if field_path == "severity":
        a_lvl = _SEVERITY_ORDER.get((str(actual or "DEFAULT")).upper(), 0)
        t_lvl = _SEVERITY_ORDER.get(value.upper(), 0)
        if op == "=":
            return a_lvl == t_lvl
        if op == "!=":
            return a_lvl != t_lvl
        if op == ">=":
            return a_lvl >= t_lvl
        if op == "<=":
            return a_lvl <= t_lvl
        if op == ">":
            return a_lvl > t_lvl
        if op == "<":
            return a_lvl < t_lvl
        return False

    # ----- None / missing field -----
    if actual is None:
        if op in ("=", ">=", "<="):
            return value in ("", "null")
        if op == "!=":
            return value not in ("", "null")
        return False

    actual_str = str(actual)

    # ----- : contains / substring -----
    if op == ":":
        return value.lower() in actual_str.lower()

    # ----- string comparison (works for ISO timestamps lexicographically) -----
    if op == "=":
        return actual_str == value
    if op == "!=":
        return actual_str != value
    if op == ">=":
        return actual_str >= value
    if op == "<=":
        return actual_str <= value
    if op == ">":
        return actual_str > value
    if op == "<":
        return actual_str < value

    return False


# ---------------------------------------------------------------------------
# AST evaluator
# ---------------------------------------------------------------------------


def _eval(node: tuple, entry: dict) -> bool:
    """Recursively evaluate an AST node against a log entry.

    Args:
        node (tuple): AST node produced by the parser.
        entry (dict): Log entry dict to match against.

    Returns:
        bool: True if the node matches the entry.
    """
    tag = node[0]
    if tag == "TRUE":
        return True
    if tag == "FALSE":
        return False
    if tag == "AND":
        return _eval(node[1], entry) and _eval(node[2], entry)
    if tag == "OR":
        return _eval(node[1], entry) or _eval(node[2], entry)
    if tag == "NOT":
        return not _eval(node[1], entry)
    if tag == "CMP":
        _, field, op, value = node
        return _compare_values(field, op, value, entry)
    if tag == "EXISTS":
        val = _get_field(entry, node[1])
        return val is not None and val != ""
    return True


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def matches(filter_str: str, entry: dict) -> bool:
    """Return True if the log entry matches the filter expression.

    An empty or whitespace-only filter matches all entries. Parse errors
    are treated as a no-filter condition (entry is included).

    Args:
        filter_str (str): Cloud Logging filter expression.
        entry (dict): Log entry dict.

    Returns:
        bool: True if the entry satisfies the filter.
    """
    filter_str = (filter_str or "").strip()
    if not filter_str:
        return True
    try:
        tokens = _tokenize(filter_str)
        ast = _Parser(tokens).parse()
        return _eval(ast, entry)
    except Exception:
        return True  # on parse error, include the entry
