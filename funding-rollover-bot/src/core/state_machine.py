"""
State machine for per-symbol bot lifecycle.
Enforces valid state transitions.
"""
from typing import Dict, Set, Tuple

from core.models import BotState
from utils.logging_utils import get_logger

logger = get_logger(__name__)

# Valid transitions: (from_state, to_state) -> True
_VALID_TRANSITIONS: Set[Tuple[BotState, BotState]] = {
    (BotState.FLAT,             BotState.WARMUP),
    (BotState.WARMUP,           BotState.PRE_FUNDING_SCAN),
    (BotState.WARMUP,           BotState.FLAT),          # warmup failure
    (BotState.PRE_FUNDING_SCAN, BotState.ARMED),
    (BotState.PRE_FUNDING_SCAN, BotState.FLAT),          # no candidate found
    (BotState.ARMED,            BotState.DECISION_LOCKED),
    (BotState.ARMED,            BotState.FLAT),          # aborted before lock
    (BotState.DECISION_LOCKED,  BotState.ENTERING),
    (BotState.DECISION_LOCKED,  BotState.COOLDOWN),      # NO_TRADE locked
    (BotState.ENTERING,         BotState.IN_POSITION),
    (BotState.ENTERING,         BotState.COOLDOWN),      # fill failed
    (BotState.IN_POSITION,      BotState.EXITING),
    (BotState.EXITING,          BotState.COOLDOWN),
    (BotState.EXITING,          BotState.FLAT),          # direct return after exit
    (BotState.COOLDOWN,         BotState.FLAT),
}


class StateMachine:
    """
    Tracks the BotState for each symbol independently.
    """

    def __init__(self) -> None:
        self._states: Dict[str, BotState] = {}

    def get_state(self, symbol: str) -> BotState:
        """Return current state for a symbol (defaults to FLAT)."""
        return self._states.get(symbol, BotState.FLAT)

    def transition(self, symbol: str, new_state: BotState) -> None:
        """
        Transition symbol to new_state.
        Raises ValueError if the transition is not valid.
        """
        current = self.get_state(symbol)
        if not self.is_valid_transition(current, new_state):
            raise ValueError(
                f"Invalid state transition for {symbol}: {current.value} -> {new_state.value}"
            )
        logger.info("State transition", symbol=symbol, from_state=current.value, to_state=new_state.value)
        self._states[symbol] = new_state

    def force_state(self, symbol: str, new_state: BotState) -> None:
        """
        Force a state transition without validation.
        Use sparingly (e.g., emergency reset).
        """
        old = self._states.get(symbol, BotState.FLAT)
        logger.warning(
            "Forced state transition",
            symbol=symbol,
            from_state=old.value,
            to_state=new_state.value,
        )
        self._states[symbol] = new_state

    def is_valid_transition(self, from_state: BotState, to_state: BotState) -> bool:
        """Return True if the transition from_state -> to_state is valid."""
        return (from_state, to_state) in _VALID_TRANSITIONS

    def reset(self, symbol: str) -> None:
        """Reset symbol to FLAT state (bypasses validation)."""
        self._states[symbol] = BotState.FLAT

    def all_states(self) -> Dict[str, str]:
        """Return a dict of symbol -> state name for logging."""
        return {sym: state.value for sym, state in self._states.items()}
