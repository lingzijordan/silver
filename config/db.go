package config

type DBcredentials struct {
	User string `json:"user"`
	Password string `json:"password"`
	Ip string `json:"ip"`
	Database string
}