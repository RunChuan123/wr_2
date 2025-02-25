package utils

import "hash/fnv"

// hash转化函数
func ToHash(text string) int {
	hash := fnv.New32a()
	hash.Write([]byte(text))
	return int(hash.Sum32()) % 100
}
