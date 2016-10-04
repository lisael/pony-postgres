primitive IdleTransction
  fun string(): String => "Idle"
primitive ActiveTransaction
  fun string(): String => "Active"
primitive ErrorTransaction
  fun string(): String => "Error"
primitive UnknownTransactionStatus
  fun string(): String => "Unknown"

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
  fun string(): String => "Unknown"

