package utils

import (
	"encoding/json"
	"io"
	"os"
)

// 读取配置文件的相应key
func ReadKey(key string) (string, bool) {

	file, err := os.Open("config/config.json")
	if err != nil {
		return "", false
	}
	defer file.Close()
	jsonData, err := io.ReadAll(file)
	if err != nil {
		return "", false
	}
	var config map[string]string
	err = json.Unmarshal(jsonData, &config)
	v, exists := config[key]
	if !exists {
		return "", false
	} else {
		return string(v), true
	}
}
