package migrate

func IsCallback(stmt string) (name string) {
	return isCallback(stmt)
}

func IsComment(stmt string) bool {
	return isComment(stmt)
}
