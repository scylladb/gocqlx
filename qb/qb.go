package qb

// Builder is interface implemented by all the builders.
type Builder interface {
	// ToCql builds the query into a CQL string and named args.
	ToCql() (stmt string, names []string)
}

// M is a map.
type M map[string]interface{}
