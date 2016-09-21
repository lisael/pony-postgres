primitive IdleTransction
primitive ActiveTransaction
primitive ErrorTransaction
primitive UnknownTransactionStatus

type TransactionStatus is (IdleTransction
                          | ActiveTransaction
                          | ErrorTransaction
                          | UnknownTransactionStatus)

primitive StatusFromByte
  fun apply(b: U8): TransactionStatus =>
    match b
    | 'I' => IdleTransction
    | 'T' => ActiveTransaction
    | 'E' => ErrorTransaction
    else
      UnknownTransactionStatus
    end


trait Message

