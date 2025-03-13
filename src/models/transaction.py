class Transaction:
    def __init__(self, from_amount, from_token, to_amount, to_token, trading_time=-1, transaction_hash="",
                 is_tracked_from=False, is_tracked_to=False, is_merged=False):
        self.from_amount = from_amount
        self.from_token = from_token
        self.to_amount = to_amount
        self.to_token = to_token
        self.is_tracked_from = is_tracked_from
        self.is_tracked_to = is_tracked_to
        self.trading_time = trading_time
        self.transaction_hash = transaction_hash
        self.is_merged = is_merged

    def __repr__(self):
        return f"{self.from_amount} {self.from_token} -> {self.to_amount} {self.to_token}"


def split_a_transaction(txn, amount_to_split, pos):
    if pos == 'to':
        if amount_to_split > txn.to_amount:
            raise ValueError("Số lượng chia tách lớn hơn số lượng giao dịch có sẵn.")
        split_ratio = amount_to_split / txn.to_amount
        new_from_amount = txn.from_amount * split_ratio
        new_txn = Transaction(new_from_amount, txn.from_token,
                              amount_to_split, txn.to_token, is_tracked_to=True)

        remaining_txn = Transaction(txn.from_amount - new_from_amount, txn.from_token,
                                    txn.to_amount - amount_to_split, txn.to_token)

        return new_txn, remaining_txn
    else:
        if amount_to_split > txn.from_amount:
            raise ValueError("Số lượng chia tách lớn hơn số lượng giao dịch có sẵn.")
        split_ratio = amount_to_split / txn.from_amount
        new_to_amount = txn.to_amount * split_ratio
        new_txn = Transaction(amount_to_split, txn.from_token,
                              new_to_amount, txn.to_token,
                              is_tracked_from=True, is_tracked_to=txn.is_tracked_to)

        remaining_txn = Transaction(txn.from_amount - amount_to_split, txn.from_token,
                                    txn.to_amount - new_to_amount, txn.to_token,
                                    is_tracked_from=txn.is_tracked_from, is_tracked_to=txn.is_tracked_to)

        return new_txn, remaining_txn


def split_transactions(transactions: list[Transaction]):
    N = len(transactions)

    epsilon = 0.0000001

    for i in range(N - 1, -1, -1):

        current_txn = transactions[i]
        if not current_txn.is_tracked_from:

            for j in range(i - 1, -1, -1):

                previous_txn = transactions[j]
                if not previous_txn.is_tracked_to:

                    if current_txn.from_token == previous_txn.to_token:
                        if abs(current_txn.from_amount - previous_txn.to_amount) < epsilon:
                            previous_txn.is_tracked_to = True
                            current_txn.is_tracked_from = True
                            tmp = transactions[0:j] + [previous_txn] + transactions[j + 1:i] + [
                                current_txn] + transactions[i + 1:N]
                            return split_transactions(tmp)

                        elif current_txn.from_amount < previous_txn.to_amount:
                            current_txn.is_tracked_from = True
                            first_split_txn, second_split_txn = split_a_transaction(previous_txn, current_txn.from_amount,
                                                                                    'to')
                            tmp = transactions[0:j] + [first_split_txn] + [second_split_txn] + transactions[j + 1:N]
                            return split_transactions(tmp)

                        elif current_txn.from_amount > previous_txn.to_amount:
                            previous_txn.is_tracked_to = True
                            first_split_txn, second_split_txn = split_a_transaction(current_txn, previous_txn.to_amount,
                                                                                    'from')
                            tmp = transactions[0:i] + [first_split_txn] + [second_split_txn] + transactions[i + 1:N]
                            return split_transactions(tmp)

    return transactions


def merge_transactions(transactions: list[Transaction]):
    N = len(transactions)

    checked = [False for i in range(N)]

    epsilon = 0.0000001

    final_transactions = []

    for i in range(N):
        if not checked[i]:
            current_txs = transactions[i]
            checking_to_amount = current_txs.to_amount
            checking_to_token = current_txs.to_token
            for j in range(i+1, N):
                tmp_txs = transactions[j]
                if not checked[j] and abs(checking_to_amount - tmp_txs.from_amount) < epsilon and checking_to_token == tmp_txs.from_token:
                    checking_to_amount = tmp_txs.to_amount
                    checking_to_token = tmp_txs.to_token
                    checked[j] = True

            checked[i] = True
            final_transactions.append(
                Transaction(current_txs.from_amount, current_txs.from_token,
                            checking_to_amount, checking_to_token)
            )
    return final_transactions