# ID Providers

Each node in Quine's graph is defined by its ID—referred to internally as `QuineId`. The ID itself is fundamentally an uninterpreted sequence of bytes, but using each ID is mediated by a class of objects referred to as `IdProviders`. ID providers make working with IDs more convenient and allow for multiple types to be used or migration from one type to another.

An ID Provider is chosen at startup for an instance of the graph. the default ID provider creates and expects to find IDs that can be read as UUIDs. This means that every node in the graph is defined by a UUID.

Different ID Providers can implement different strategies for allocating new IDs. For instance, alternate UUID providers can be configured to generate UUIDs which conform to specification of UUID versions 3, 4, or 5.

## Supported ID Providers

- `uuid` - This ID provider will generate RFC compliant UUIDs, but will allow for reading a looser interpretation of UUIDs which allows for any use of the 128 bits available in a UUID. This is the default ID provider.
- `uuid-3` - Generate and read only Version-3 compliant UUIDs.
- `uuid-4` - Generate and read only Version-4 compliant UUIDs. When returning random UUIDs, and using `idFrom` (described below), deterministic UUIDs with Version-4 identifying bytes will be used.
- `uuid-5` - Generate and read only Version-5 compliant UUIDs 
- `long` - Generate random integer IDs in the range: [-(2^53-1), 2^53-1] -- these may be safely used as IEEE double-precision floating-point values without loss of precision. This id scheme is not appropriate for large-scale datasets because of the high likelihood of a collision 
- `byte-array` - generate unstructured byte arrays as IDs.

## idFrom(…)

Quine has a unique challenge: how to work statefully with potentially infinite streams of data. One key strategy Quine uses is to generate known IDs from data deterministically. The `idFrom` function does exactly that. `idFrom` is a function we've added to Cypher which will take any number of arguments and deterministically produce a ID from that data. This is similar to a consistent-hashing strategy, except that the ID produced from this function is always an ID that conforms to the type chosen for the ID provider.

For example, if data ingested from a stream needs to set a new telephone number for a customer, the Cypher ingest query would use `idFrom` to locate the relevant node in the graph and update its property like this:

```cypher
MATCH (customer) 
WHERE id(customer) = idFrom('customer', $that.customer_id)
SET customer.phone = $that.new_phone_number
```

In this example, `idFrom('customer', $that.customer_id)` is used to generate a specific ID for a single node in the graph, determined by the constant string `'customer'` and the value of `customer_id` being read from the newly streamed record. The ID returned from this function call will be a single fixed ID in the space of which ever ID provider was chosen (`long`, `uuid`, etc.). That ID is used in the `WHERE` clause to select a specific node from the graph and Quine is able to perform the corresponding update very efficiently.

@@@ note

See the section on @ref:[Querying Infinite Data](../getting_started/querying_infinite_data.md) for a detailed discussion of how to use `idFrom` to work with infinite data streams during ingest, standing queries, and ad hoc queries.

@@@
