package services

import (
	"database/sql"
	_ "github.com/lib/pq"
	"fmt"
	"github.com/yewno/log"
	"github.com/yewno/silver/config"
	"errors"
	"encoding/json"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}


func login(cred *config.DBcredentials) string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", cred.User, cred.Password, cred.Ip, cred.Database)
}

func ConnectDB(cred *config.DBcredentials, dbType string) (*sql.DB, error) {
	db, err := sql.Open(dbType, login(cred))
	if err != nil {
		log.WithError(err)
	}
	//defer db.Close()

	err = db.Ping()
	if err != nil {
		log.WithError(err)
	} else {
		log.Infof("ping %s is successful!", dbType)
		//fmt.Printf("ping is successful!!!\n")
	}

	return db, err
}

func GetDBCredentials(cfg *config.Config, dbcred *config.DBcredentials) error {

	conn, err := NewSilverDynamo(cfg)
	if err != nil {
		log.WithError(err).Error("")
		return err
	}

	query := map[string]string{
		"db_type":  cfg.DBType,
	}

	result := map[string]interface{}{}
	ok, err := conn.Get(cfg.ConfigTbl, query, &result)
	if err != nil {
		log.WithError(err).Error("")
		return err
	}
	if !ok {
		err := errors.New(fmt.Sprintf("config not found for %s", cfg.DBType))
		log.WithError(err).Error("")
		return err
	}
	bytesArr, err := json.Marshal(result)
	if err != nil {
		log.WithError(err).Error("")
		return err
	}
	if err := json.Unmarshal(bytesArr, dbcred); err != nil {
		log.WithError(err).Error("")
		return err
	}

	//spew.Dump(dbcred)

	return nil
}