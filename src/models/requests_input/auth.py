from typing import Optional

from pydantic import BaseModel


class AuthBody(BaseModel):
    walletType: Optional[str] = None
    address: str
    signature: str
    nonce: int


class RegisterBody(BaseModel):
    userName: str
    password: str
    fullName: str
    email: Optional[str] = None


class LoginBody(BaseModel):
    userName: str
    password: str


class DepositWithdrawBody(BaseModel):
    vaultAddress: str
    amountCollateral: int


class UpdateDepositBody(BaseModel):
    tx: str

