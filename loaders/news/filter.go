package news

import "strings"

func FilterOnTopics(topics string) bool {
	uselessTopics := []string{"ENT", "FILM", "LIF", "MUSIC", "ODD", "PRO", "WEA", "SPO"}
	for _, topic := range uselessTopics {
		if strings.Contains(topics, topic) {
			return true
		}
	}
	return false
}
