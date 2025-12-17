from dataclasses import dataclass

@dataclass
class Position:
    entry_price: float
    size: float
    direction: str  # "long" or "short"

class Simulator:
    def __init__(self, initial_balance: float):
        self.balance = initial_balance
        self.equity = initial_balance
        self.positions = {}  # symbol -> Position

    def open_position(self, symbol, price, size, direction):
        self.positions[symbol] = Position(price, size, direction)

    def close_position(self, symbol, price):
        pos = self.positions.pop(symbol, None)
        if not pos:
            return 0.0
        pnl = (price - pos.entry_price) * pos.size
        if pos.direction == "short":
            pnl = (pos.entry_price - price) * pos.size
        self.equity += pnl
        return pnl
