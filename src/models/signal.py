from typing import Optional

from pydantic import BaseModel


class WebsocketSignalsQuery(BaseModel):
    chainId: Optional[str] = None
    type: Optional[str] = None
    filter: Optional[str] = None
