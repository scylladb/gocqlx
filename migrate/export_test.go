package migrate

func IsCallback(stmt string) (name string) {
	return isCallback(stmt)
}
