package news

import "github.com/yewno/silver/formats"

func PickOne(s1, s2 string) string {
	if s1 == "" {
		return s2
	} else {
		return s1
	}
}

func MergeEntries(entry1, entry2 *formats.NewsContentMeta) *formats.NewsContentMeta {
	var headline, fulltext, types, keywords string
	headline = PickOne(entry1.Headline, entry2.Headline)
	fulltext = PickOne(entry1.FullText, entry2.FullText)
	keywords = PickOne(entry1.Keywords, entry2.Keywords)

	if headline != "" && fulltext != "" {
		types = "FULLTEXT"
	} else {
		types = entry1.Type
	}

	return &formats.NewsContentMeta{
		YId:                entry1.YId,
		Created:            entry1.Created,
		Date:               entry1.Date,
		Day:                entry1.Day,
		Month:              entry1.Month,
		Year:               entry1.Year,
		Language:           entry1.Language,
		Headline:           headline,
		FullText:           fulltext,
		Type:               types,
		IngestedAt:         entry1.IngestedAt,
		IndexedAt:          entry1.IndexedAt,
		ProcessedAt:        entry1.ProcessedAt,
		Source:             entry1.Source,
		Title:              entry1.Title,
		Keywords:           keywords,
		Topics:             entry1.Topics,
		NamedItems:         entry1.NamedItems,
		NamedItemsOriginal: entry1.NamedItemsOriginal,
	}
}
