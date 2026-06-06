package fluvio

// Tx is an opaque transaction handle for EnqueueTx.
// The postgres driver accepts github.com/jackc/pgx/v5.Tx values.
type Tx interface{}
