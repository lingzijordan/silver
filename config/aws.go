package config

import (
	"github.com/aws/aws-sdk-go/aws/credentials"
)

type Config struct {
	Developer       bool
	Bucket          string
	Credentials     *credentials.Credentials
	ProcessedQueue  string
	Region          string
	ConfigTbl       string
	StatsTbl        string
	DBtable         string
	Source          string
	DBType          string
}
