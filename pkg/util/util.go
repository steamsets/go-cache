package util

func MapToString(m map[string]struct{}) []string {
	ret := make([]string, 0)
	for k := range m {
		ret = append(ret, k)
	}
	return ret
}
