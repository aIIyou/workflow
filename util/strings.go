package util

import "strings"

func Pascal(s string) string {
	if s == "" {
		return ""
	}

	// 分割下划线
	words := strings.Split(s, "_")
	for i, word := range words {
		if len(word) > 0 {
			// 将每个单词的首字母大写
			words[i] = strings.ToUpper(string(word[0])) + word[1:]
		}
	}
	return strings.Join(words, "")
}
