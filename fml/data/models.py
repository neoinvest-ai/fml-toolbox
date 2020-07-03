from collections import namedtuple

Tick = namedtuple('Tick', ('time', 'price', 'bid', 'ask', 'quantity'))
Quote = namedtuple('Quote', ('price', 'bid', 'ask'))
